(ns hive-dvergr.schema
  "Closed Malli contracts for the experimental run boundary."
  (:require [hive-dsl.result :as r]
            [hive-schemas.schema :as hs]))

(def NonBlankString
  [:and
   [:string {:min 1 :max 512}]
   [:fn {:error/message "must contain a non-whitespace character"}
    #(boolean (and (string? %) (re-find #"\S" %)))]] )

(def ContentHash
  [:re #"^sha256:[0-9a-f]{64}$"])

(def RunStatus
  [:enum :run/submitted :run/running :run/completed :run/failed
   :run/cancelled :run/interrupted])

(def TerminalStatus
  [:enum :run/completed :run/failed :run/cancelled :run/interrupted])

(def EventType
  [:enum :run/submitted :run/started :run/completed :run/failed
   :run/cancel-requested :run/cancelled :run/interrupted])

(def RunRequest
  [:map {:closed true}
   [:run/id NonBlankString]
   [:task/id NonBlankString]
   [:agent/id NonBlankString]
   [:run/attempt [:int {:min 0}]]
   [:run/input [:map {:closed false}]]])

(def RunHandle
  [:map {:closed true}
   [:run/id NonBlankString]
   [:task/id NonBlankString]
   [:agent/id NonBlankString]
   [:run/attempt [:int {:min 0}]]])

(def RunEvent
  [:map {:closed true}
   [:event/id NonBlankString]
   [:event/type EventType]
   [:event/at int?]
   [:run/id NonBlankString]
   [:task/id NonBlankString]
   [:agent/id NonBlankString]
   [:run/attempt [:int {:min 0}]]
   [:run/sequence [:int {:min 0}]]
   [:event/payload [:map {:closed false}]]])

(def RunOutcome
  [:map {:closed true}
   [:run/id NonBlankString]
   [:task/id NonBlankString]
   [:agent/id NonBlankString]
   [:run/attempt [:int {:min 0}]]
   [:run/status TerminalStatus]
   [:run/result [:map {:closed false}]]
   [:run/result-hash ContentHash]
   [:run/completed-at int?]])

(def schemas
  {:hive-dvergr/non-blank-string NonBlankString
   :hive-dvergr/content-hash ContentHash
   :hive-dvergr/run-status RunStatus
   :hive-dvergr/terminal-status TerminalStatus
   :hive-dvergr/event-type EventType
   :hive-dvergr/run-request RunRequest
   :hive-dvergr/run-handle RunHandle
   :hive-dvergr/run-event RunEvent
   :hive-dvergr/run-outcome RunOutcome})

(defn install!
  "Install or refresh hive-dvergr's registry contribution."
  []
  (hs/register-all! schemas))

(defn uninstall!
  "Remove hive-dvergr's registry contribution."
  []
  (hs/deregister-all! (keys schemas)))

(defn valid?
  [schema-key value]
  (hs/validate schema-key value))

(defn promote-request
  "Promote an external request to the closed RunRequest value."
  [request]
  (if (valid? :hive-dvergr/run-request request)
    (r/ok (select-keys request [:run/id :task/id :agent/id :run/attempt :run/input]))
    (r/err :hive-dvergr/invalid-run-request
           {:request request})))
