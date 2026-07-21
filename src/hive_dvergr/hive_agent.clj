(ns hive-dvergr.hive-agent
  "Dynamic compatibility adapter for Hive's provisional agent protocols.

  The protocols currently live in hive-mcp. This namespace deliberately uses
  requiring-resolve + `extend` so hive-dvergr has no production dependency on
  hive-mcp or the proprietary hive-agent artifact."
  (:require [hive-dsl.result :as r]
            [hive-dvergr.ports :as ports]))

(defrecord DvergrAgenticLoop
           [runtime ling-id config current-run constraints messages])

(defonce ^:private installed? (atom false))

(defn- resolve-var [sym]
  (try (requiring-resolve sym) (catch Throwable _ nil)))

(defn- session-state-value [status]
  (if-let [ctor (resolve-var 'hive-mcp.agent.session-state/agent-session-state)]
    (ctor status)
    status))

(defn- request-for [loop start-config]
  (let [cfg (merge (:config loop) start-config)
        run-id (str (or (:run-id cfg) (random-uuid)))
        task-id (str (or (:task-id cfg) (random-uuid)))]
    {:run/id run-id
     :task/id task-id
     :agent/id (str (:ling-id loop))
     :run/attempt (long (or (:attempt cfg) 0))
     :run/input (dissoc cfg :run-id :task-id :attempt)}))

(defn- start-loop! [loop start-config]
  (let [request (request-for loop start-config)
        started (ports/start-run! (:runtime loop) request)]
    (if (r/ok? started)
      (do (reset! (:current-run loop) (:run/id (:ok started)))
          {:session-id (:run/id (:ok started))})
      {:error true :result (pr-str started)})))

(defn- abort-loop! [loop]
  (if-let [run-id @(:current-run loop)]
    (let [cancelled (ports/cancel-run! (:runtime loop) run-id)]
      {:aborted? (r/ok? cancelled) :run-id run-id})
    {:aborted? false :reason :idle}))

(defn- loop-session-state [loop]
  (if-let [run-id @(:current-run loop)]
    (let [record (ports/run-status (:runtime loop) run-id)
          status (when (r/ok? record) (:run/status (:ok record)))]
      (session-state-value
       (case status
         (:run/submitted :run/running) :session/running
         :run/completed :session/done
         :run/failed :session/errored
         (:run/cancelled :run/interrupted) :session/aborted
         :session/idle)))
    (session-state-value :session/idle)))

(defn- collect-loop! [loop opts]
  (if-let [run-id @(:current-run loop)]
    (let [outcome (ports/await-run! (:runtime loop) run-id opts)]
      (if (r/ok? outcome)
        {:result (:run/result (:ok outcome))
         :turns 1
         :tool-calls-made 0
         :dvergr/run-id run-id
         :dvergr/result-hash (:run/result-hash (:ok outcome))}
        {:error true :result (pr-str outcome) :turns 0 :tool-calls-made 0}))
    {:error true :result "No active dvergr run" :turns 0 :tool-calls-made 0}))

(defn- loop-transcript [loop]
  (if-let [run-id @(:current-run loop)]
    (let [events (ports/run-events (:runtime loop) run-id)]
      (if (r/ok? events) (:ok events) []))
    []))

(defn install-protocol!
  "Dynamically extend DvergrAgenticLoop when Hive's current protocol is present."
  []
  (if @installed?
    {:installed? true :already-installed? true}
    (if-let [protocol-var (resolve-var 'hive-mcp.agent.agentic-loop/IAgenticLoop)]
      (do
        (extend DvergrAgenticLoop
          @protocol-var
          {:start! (fn [this start-config] (start-loop! this start-config))
           :abort! (fn [this] (abort-loop! this))
           :session-state (fn [this] (loop-session-state this))
           :send-message! (fn [this message]
                            (swap! (:messages this) conj message)
                            {:sent? true})
           :collect-response! (fn [this opts] (collect-loop! this opts))
           :cost (fn [this]
                   {:total-cost-usd 0.0
                    :turns (if @(:current-run this) 1 0)})
           :transcript (fn [this] (loop-transcript this))
           :tool-results! (fn [this results]
                            (swap! (:messages this) into results)
                            {:accepted? true})
           :hooks (fn [_] #{:cap/datahike :cap/durable-results})
           :constrain! (fn [this new-constraints]
                         (swap! (:constraints this) merge new-constraints)
                         {:applied? true})})
        (reset! installed? true)
        {:installed? true})
      {:installed? false :reason :hive-agent-protocol-unavailable})))

(defn make-loop-factory
  "Return `(fn [ling-id config] -> IAgenticLoop)` when the host protocol exists."
  [runtime]
  (fn [ling-id config]
    (install-protocol!)
    (->DvergrAgenticLoop runtime ling-id config
                         (atom nil) (atom {}) (atom []))))

(defn make-backend
  "Use hive-agent's existing headless adapter when it is present in the host."
  [runtime]
  (install-protocol!)
  (when-let [make-agentic-backend
             (resolve-var 'hive-agent.loop.headless-adapter/make-agentic-loop-backend)]
    (make-agentic-backend
     (make-loop-factory runtime)
     :dvergr
     #{:cap/datahike :cap/durable-results :cap/cancellable})))

(defn integration-status []
  {:agentic-loop-protocol? (boolean
                            (resolve-var 'hive-mcp.agent.agentic-loop/IAgenticLoop))
   :headless-adapter? (boolean
                       (resolve-var 'hive-agent.loop.headless-adapter/make-agentic-loop-backend))
   :installed? @installed?})
