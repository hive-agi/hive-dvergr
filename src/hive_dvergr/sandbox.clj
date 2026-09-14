(ns hive-dvergr.sandbox
  "Capability-free cljw evaluation in a fresh Wasmtime process.
   Configuration belongs to the trusted host; requests supply only source."
  (:require [clojure.java.io :as io])
  (:import [java.io InputStream]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files Path]
           [java.nio.file.attribute FileAttribute PosixFilePermissions]
           [java.util.concurrent TimeUnit]))

(def default-limits
  {:fuel 1000000000
   :memory-bytes 268435456
   :timeout-ms 30000
   :output-bytes 65536
   :input-bytes 65536})

(defn- reject! [reason data]
  (throw (ex-info (str "cljw sandbox: " (name reason))
                  (assoc data :reason reason))))

(defn- absolute-file [value executable?]
  (let [file (when (string? value) (io/file value))]
    (when-not (and file (.isAbsolute file) (.isFile file)
                   (or (not executable?) (.canExecute file)))
      (reject! :invalid-runtime-path {:path value :executable? executable?}))
    (.getCanonicalPath file)))

(defn configuration
  "Validate host-owned paths and finite resource limits. No arbitrary CLI flags."
  [options]
  (let [known (into #{:wasmtime :module} (keys default-limits))
        unknown (seq (remove known (keys options)))
        limits (merge default-limits (select-keys options (keys default-limits)))]
    (when unknown (reject! :unknown-options {:keys (vec unknown)}))
    (doseq [[k v] limits]
      (when-not (and (integer? v) (pos? v) (< v Integer/MAX_VALUE))
        (reject! :invalid-limit {:key k :value v})))
    (assoc limits
           :wasmtime (absolute-file (:wasmtime options) true)
           :module (absolute-file (:module options) false))))

(defn- empty-directory! []
  (let [path (Files/createTempDirectory "hive-cljw-sandbox-"
                                        (make-array FileAttribute 0))]
    (try
      (Files/setPosixFilePermissions path (PosixFilePermissions/fromString "r-x------"))
      (when (Files/isWritable path)
        (reject! :writable-sandbox-directory {}))
      path
      (catch Exception e
        (Files/deleteIfExists path)
        (throw e)))))

(defn- command [config directory]
  [(:wasmtime config) "run"
   "-W" (str "fuel=" (:fuel config)
             ",max-memory-size=" (:memory-bytes config)
             ",max-table-elements=100000,trap-on-grow-failure=y")
   "-S" "inherit-network=n,inherit-env=n,tcp=n,udp=n,allow-ip-name-lookup=n,http=n"
   "--dir" (str directory "::.")
   (:module config) "-"])

(defn- process! [config directory]
  (let [builder (ProcessBuilder. ^java.util.List (command config directory))]
    (.clear (.environment builder))
    (.directory builder (.toFile ^Path directory))
    (.redirectErrorStream builder true)
    (.start builder)))

(defn- worker [f]
  (let [result (promise)
        thread (Thread. ^Runnable
                        (fn []
                          (deliver result
                                   (try {:value (f)}
                                        (catch Exception e {:exception e})))))]
    (.setDaemon thread true)
    (.start thread)
    result))

(defn- collect-output! [process limit exceeded?]
  (with-open [stream (.getInputStream ^Process process)]
    (let [bytes (.readNBytes ^InputStream stream (int (inc limit)))]
      (when (> (alength bytes) limit)
        (reset! exceeded? true)
        (.destroyForcibly ^Process process))
      (String. bytes 0 (min limit (alength bytes)) StandardCharsets/UTF_8))))

(defn- send-source! [process source]
  (with-open [stream (.getOutputStream ^Process process)]
    (.write stream ^bytes source)))

(defn- stop! [process]
  (when (.isAlive ^Process process)
    (.destroyForcibly ^Process process))
  (.waitFor ^Process process 1000 TimeUnit/MILLISECONDS))

(defn- outcome! [process output writer exceeded? config]
  (let [finished? (.waitFor ^Process process
                            (long (:timeout-ms config)) TimeUnit/MILLISECONDS)
        _ (when-not finished? (stop! process))
        captured (deref output 1000 nil)
        written (deref writer 1000 nil)
        result {:sandbox :cljw-wasmtime
                :exit (when finished? (.exitValue ^Process process))
                :output (or (:value captured) "")}]
    (cond
      @exceeded? (reject! :output-limit result)
      (not finished?) (reject! :timeout result)
      (not (zero? (:exit result))) (reject! :guest-failed result)
      (or (nil? captured) (:exception captured)
          (nil? written) (:exception written)) (reject! :io-failed result)
      :else result)))

(defn- evaluate-source! [config source]
  (let [directory (empty-directory!)]
    (try
      (let [process (process! config directory)]
        (try
          (let [exceeded? (atom false)
                output (worker #(collect-output! process (:output-bytes config) exceeded?))
                writer (worker #(send-source! process source))]
            (outcome! process output writer exceeded? config))
          (finally (stop! process))))
      (finally (Files/deleteIfExists directory)))))

(defn runner
  "Return a dvergr :runner accepting {:run/input {:code source-string}}.
   Each invocation gets a fresh Wasm instance and an empty read-only directory.
   Missing runtime, denied capabilities and exhausted limits throw; no native fallback."
  [options]
  (let [config (configuration options)]
    (fn [request]
      (let [code (get-in request [:run/input :code])]
        (when-not (string? code)
          (reject! :missing-code {}))
        (let [source (.getBytes ^String code StandardCharsets/UTF_8)]
          (when (> (alength source) (:input-bytes config))
            (reject! :input-limit {}))
          (evaluate-source! config source))))))
