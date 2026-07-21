(ns hive-dvergr.ports
  "Host-neutral ports. dvergr, Datahike, and Hive protocols stay in adapters."
  (:refer-clojure :exclude [await]))

(defprotocol IRunLedger
  (begin-run! [ledger request]
    "Persist the run identity before execution begins.")
  (append-event! [ledger event]
    "Append one monotonic, correlated lifecycle event.")
  (record-outcome! [ledger outcome]
    "Persist the full terminal outcome; previews are never authoritative.")
  (run-events [ledger run-id]
    "Return ordered lifecycle events for run-id.")
  (run-record [ledger run-id]
    "Return current durable run metadata, including terminal outcome when present.")
  (close-ledger! [ledger]
    "Idempotently release backing resources."))

(defprotocol IRunRuntime
  (start-run! [runtime request]
    "Start a validated run and return Result<RunHandle>.")
  (await-run! [runtime run-id opts]
    "Await or recover the terminal RunOutcome by durable run identity.")
  (cancel-run! [runtime run-id]
    "Request cancellation and durably terminalize the run.")
  (run-status [runtime run-id]
    "Read durable run status without requiring a live execution handle.")
  (shutdown-runtime! [runtime]
    "Cancel live handles and release dvergr rooms."))
