(ns hive-dvergr.sandbox-test
  (:require [clojure.test :refer [deftest is testing run-tests]]
            [hive-dvergr.sandbox :as sandbox])
  (:import [java.net InetAddress ServerSocket SocketTimeoutException]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn config []
  {:wasmtime (System/getenv "HIVE_WASMTIME")
   :module (System/getenv "HIVE_CLJW_WASM")})

(defn evaluate
  ([source] (evaluate {} source))
  ([limits source]
   ((sandbox/runner (merge (config) limits)) {:run/input {:code source}})))

(defn failure [f]
  (try (f) nil
       (catch clojure.lang.ExceptionInfo e (ex-data e))))

(deftest configuration-fails-closed
  (is (= :invalid-runtime-path
         (:reason (failure #(sandbox/runner (assoc (config) :wasmtime "/nonexistent/wasmtime"))))))
  (is (= :unknown-options
         (:reason (failure #(sandbox/runner (assoc (config) :args ["--dir" "/"]))))))
  (is (= :invalid-limit
         (:reason (failure #(sandbox/runner (assoc (config) :fuel 0))))))
  (is (= :missing-code
         (:reason (failure #((sandbox/runner (config)) {:run/input {:task "ignored"}})))))
  (is (= :input-limit
         (:reason (failure #(evaluate {:input-bytes 1} "(println 42)"))))))

(deftest evaluates-and-isolates-runs
  (is (= "4950\n" (:output (evaluate "(println (reduce + (range 100)))"))))
  (evaluate "(def run-private-value 41)")
  (is (= "nil\n" (:output (evaluate "(println (resolve 'run-private-value))"))))
  (is (= "nil\n" (:output (evaluate "(println (System/getenv \"HIVE_SANDBOX_SENTINEL\"))")))))

(deftest filesystem-is-not-the-host
  (let [sentinel (Files/createTempFile "hive-host-sentinel-" ".txt"
                                        (make-array FileAttribute 0))
        secret (str "host-only-" (random-uuid))]
    (try
      (spit (str sentinel) secret)
      (doseq [path [(str sentinel) (str "../../" (subs (str sentinel) 1))]]
        (let [result (failure #(evaluate (str "(println (slurp " (pr-str path) "))")))]
          (is (= :guest-failed (:reason result)))
          (is (not (.contains (or (:output result) "") secret)))))
      (is (= :guest-failed
             (:reason (failure #(evaluate (str "(spit " (pr-str (str sentinel)) " \"changed\")"))))))
      (is (= secret (slurp (str sentinel))))
      (is (= :guest-failed
             (:reason (failure #(evaluate "(spit \"guest-write\" \"no\")")))))
      (finally (Files/deleteIfExists sentinel)))))

(deftest no-host-process-execution
  (is (= :guest-failed
         (:reason
          (failure #(evaluate "(require '[clojure.java.shell :as sh]) (sh/sh \"sh\" \"-c\" \"echo escaped\")"))))))

(deftest network-cannot-reach-host
  (with-open [listener (ServerSocket. 0 1 (InetAddress/getByName "127.0.0.1"))]
    (.setSoTimeout listener 200)
    (let [source (str "(cljw.http.client/get \"http://127.0.0.1:"
                      (.getLocalPort listener) "/\")")
          result (failure #(evaluate source))]
      (is (= :guest-failed (:reason result)) (pr-str result))
      (is (try
            (with-open [_ (.accept listener)] false)
            (catch SocketTimeoutException _ true))
          "No connection reaches the host listener"))))

(deftest interrupt-kills-the-wasmtime-process
  (let [process-var (ns-resolve 'hive-dvergr.sandbox 'process!)
        original @process-var
        started (promise)
        ended (promise)]
    (with-redefs-fn
      {process-var (fn [config directory]
                     (let [process (original config directory)]
                       (deliver started process)
                       process))}
      (fn []
        (let [thread (Thread. ^Runnable
                              (fn []
                                (deliver ended
                                         (try (evaluate "(loop [] (recur))")
                                              (catch InterruptedException _ :interrupted)))))]
          (.start thread)
          (try
            (let [process (deref started 5000 nil)]
              (is (some? process))
              (.interrupt thread)
              (is (= :interrupted (deref ended 5000 :still-running)))
              (when process (is (not (.isAlive ^Process process)))))
            (finally
              (.interrupt thread)
              (.join thread 5000))))))))

(deftest resource-exhaustion-is-terminal
  (testing "fuel traps a running infinite loop"
    (let [result (failure #(evaluate {:fuel 100000000} "(loop [] (recur))"))]
      (is (= :guest-failed (:reason result)))
      (is (re-find #"fuel" (:output result)))))
  (testing "linear memory cannot exceed host policy"
    (is (= :guest-failed
           (:reason (failure #(evaluate {:memory-bytes 65536} "(println 42)"))))))
  (testing "host timeout includes module compilation and stdin delivery"
    (is (= :timeout
           (:reason (failure #(evaluate {:timeout-ms 1} "(loop [] (recur))"))))))
  (testing "output flooding is stopped and retained evidence stays bounded"
    (let [result (failure #(evaluate {:output-bytes 128}
                                    "(dotimes [i 10000] (println \"xxxxxxxxxxxxxxxx\"))"))]
      (is (= :output-limit (:reason result)))
      (is (<= (count (:output result)) 128)))))

(defn -main [& _]
  ;; This suite deliberately fails if a real Wasmtime/cljw artifact is absent.
  (let [result (run-tests 'hive-dvergr.sandbox-test)]
    (shutdown-agents)
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
