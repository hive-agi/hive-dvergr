(ns hive-dvergr.sandbox-integration-test
  "Cold-process integration: real dvergr handles and a disposable Datahike store."
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest is run-tests]]
            [datahike.api :as d]
            [hive-addon.protocol :as addon]
            [hive-dvergr.addon :as adapter]
            [hive-dvergr.datahike-ledger :as ledger]
            [hive-dvergr.ports :as ports]
            [hive-dvergr.sandbox-test :as sandbox-test]))

(deftest sandbox-tool-persists-success-and-failure
  (let [cfg (ledger/memory-config)
        opened (ledger/make-ledger cfg)
        run-ledger (:ok opened)
        fallback-called? (atom false)
        instance (adapter/make-addon
                  {:ledger run-ledger
                   :sandbox (sandbox-test/config)
                   :runner (fn [_] (reset! fallback-called? true))})]
    (is (some? run-ledger) (pr-str opened))
    (try
      (is (:success? (addon/initialize! instance {})))
      (let [handler (:handler (first (addon/tools instance)))
            invoke (fn [code]
                     (-> (handler {"task" "sandbox verification" "code" code})
                         :content first :text edn/read-string :ok))
            succeeded (invoke "(println (+ 20 22))")
            failed (invoke "(spit \"forbidden\" \"no\")")
            success-row (:ok (ports/run-record run-ledger (:run/id succeeded)))
            failure-row (:ok (ports/run-record run-ledger (:run/id failed)))]
        (is (= :run/completed (:run/status succeeded)) (pr-str succeeded))
        (is (= "42\n" (get-in success-row [:run/result :output])))
        (is (= :run/failed (:run/status failed)) (pr-str failed))
        (is (= :guest-failed
               (get-in failure-row [:run/result :sandbox-failure :reason])))
        (is (string? (get-in failure-row [:run/result :sandbox-failure :output])))
        (is (= [:run/submitted :run/started :run/failed]
               (mapv :event/type (:ok (ports/run-events run-ledger (:run/id failed))))))
        (is (false? @fallback-called?)))
      (finally
        (addon/shutdown! instance)
        (when run-ledger (ports/close-ledger! run-ledger))
        (d/delete-database cfg)))))

(deftest invalid-sandbox-never-falls-back
  (let [cfg (ledger/memory-config)
        run-ledger (:ok (ledger/make-ledger cfg))
        fallback-called? (atom false)
        instance (adapter/make-addon
                  {:ledger run-ledger
                   :sandbox (assoc (sandbox-test/config) :wasmtime "/missing/wasmtime")
                   :runner (fn [_] (reset! fallback-called? true))})]
    (try
      (is (false? (:success? (addon/initialize! instance {}))))
      (is (empty? (addon/hooks instance)))
      (is (false? @fallback-called?))
      (finally
        (addon/shutdown! instance)
        (ports/close-ledger! run-ledger)
        (d/delete-database cfg)))))

(defn -main [& _]
  (let [result (run-tests 'hive-dvergr.sandbox-integration-test)]
    (shutdown-agents)
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
