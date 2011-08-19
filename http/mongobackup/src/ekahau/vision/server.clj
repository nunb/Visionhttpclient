(ns ekahau.vision.server
  (:require
    [ekahau.vision.database :as database])
  (:use
    [clojure.contrib.logging :only [debug info warn error]]
    [ring.adapter.jetty :only [run-jetty]]
    [ekahau.string :only [exception->str]]
    [ekahau.vision.properties :only [load-erc-config!
                                     load-db-config!
                                     load-vision-config!]]
    [ekahau.vision.routes :only [create-routes]]
    [ekahau.engine.connection :only [create-engine-connection
                                     with-engine-connection]]
    [ekahau.vision.engine-service :only [get-erc-status!
                                         load-model-to-database!]]
    [ekahau.vision.location-service :only [create-location-manager
                                           start-tracking-locations!
                                           start-handling-locations!
                                           stop-tracking-locations!
                                           stop-handling-locations!]]
    [ekahau.vision.event-service :only [create-event-manager
                                        start-tracking-events!
                                        start-handling-events!
                                        stop-tracking-events!
                                        stop-handling-events!]])
  (:import
    [java.io File]
    [java.util Date]
    [org.mortbay.jetty Server])

  (:gen-class))

;(set! *warn-on-reflection* true)

(declare *server-agent* *vision-status* *vision-routes*)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;; Vision Status ;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defonce *vision-status* {:Vision {:server "Not yet started."
                                   :routes "Unavailable"}})

(defn update-status! [f & args] (apply alter-var-root #'*vision-status* f args))

(defn update-server-status! [msg] (update-status! assoc-in [:Vision :server] msg))

(defn update-routes-status! [msg] (update-status! assoc-in [:Vision :routes] msg))

(defn- update-status-as-failed! [thing msg e]
  (update-status! #(-> %
                     (assoc-in [:Vision thing] msg)
                     (update-in [:Problems]
                       conj (str msg " :" (exception->str e)))
                     (assoc-in [:All-OK] false))))

(def update-routes-status-as-failed! (partial update-status-as-failed! :routes))

(def update-server-status-as-failed! (partial update-status-as-failed! :server))

(defn get-status [] *vision-status*)

(defn server-running? [] (= "Running" (get-in (get-status) [:Vision :server])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;; Vision Routes ;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defonce *vision-routes* (create-routes nil nil false))

(defn- update-routes! [new-routes] (alter-var-root #'*vision-routes* (constantly new-routes)))

(defn reset-vision-routes!
  ([{:keys [db engine] :as server} server-available?]
    (try
      (update-routes! (create-routes db engine server-available?))
      (update-routes-status! (if server-available? "Enabled" "Disabled"))
      (catch Exception e
        (error "Error resetting vision routes" e)
        (update-routes-status-as-failed! "Error resetting vision routes" e))))
  ([]
    (reset-vision-routes! @*server-agent* true)))

(defn get-vision-routes-var [] #'*vision-routes*)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;; Vision Server ;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- create-vision-engine
  [db erc-config]
  (let [erc-connection (create-engine-connection erc-config)]
    {:erc-config       erc-config
     :erc-connection   erc-connection
     :event-manager    (create-event-manager erc-config)
     :location-manager (create-location-manager erc-config)}))

(defn create-server-state!
  [config]
  (let [db (database/create-vision-database! (:db config))
        engine (create-vision-engine db (:erc config))]
    {:config config
     :db     db
     :engine engine}))

; Server is an agent to serialze stops, runs, re-runs.
; Especially needed to serialize re-runs due to model changes
(defonce *server-agent* (agent nil))

(declare stop-vision-server! run-vision-server! rr-vision-server!)

(defn stop-vision-server!
  ([server]
    (try
      (info "Stopping vision-server ...")
      (let [{db :db {:keys [event-manager location-manager]} :engine} server]
        (reset-vision-routes! server false) ; Make routes 503
        (stop-tracking-locations! location-manager)
        (stop-handling-locations! location-manager)
        (stop-tracking-events! event-manager)
        (stop-handling-events! event-manager)
        (database/shutdown-db! db)
        (update-server-status! "Stopped")
        (info "Vision Server stopped successfully.")
        server)
      (catch Exception e
        (error "Error stopping vision server" e)
        (update-server-status-as-failed! "Vision Server Stop failed" e)
        server)))
  ([]
    (send *server-agent* stop-vision-server!)))

(defn run-vision-server!
  ([server config should-load-model]
    (try
      (info "Starting vision-server ...")
      (println "the server info is : " server) 
      (let [server (create-server-state! config)
            {db :db {:keys [erc-connection event-manager location-manager]} :engine} server]
        (with-engine-connection erc-connection
          ; Start tracking stuff from ERC
          (start-tracking-events! event-manager)
          (start-tracking-locations! location-manager)
          ; Load model
          (when should-load-model
            (load-model-to-database! db))
          ; Start Handling stuff from ERC
          (println "going to start handling stuff from ERC") 
          (start-handling-locations! location-manager db nil)
          (start-handling-events! event-manager db
            {:model-change-handler! (fn [& args]
                                      (warn (str "Model changed: " args))
                                      @(future (rr-vision-server!)))}))
        (reset-vision-routes! server true) ; Make routes proper
        (update-server-status! "Running")
        (info "Vision Server ran successfully.")
        server)
      (catch Exception e
        (error "Error running vision server" e)
        (reset-vision-routes! server false) ; Make routes 503
        (update-server-status-as-failed! "Vision Server Run failed" e)
        server)))
  ([server config]
    (run-vision-server! server config true))
  ([config]
    (println "comes here first the agent is " *server-agent*) 
    (send *server-agent* run-vision-server! config)))

(defn rr-vision-server!
  ([server]
    (let [{:keys [config]} server]
      (-> server
        (stop-vision-server!)
        (run-vision-server! config))))
  ([]
    (send *server-agent* rr-vision-server!)))

;this function gets the external status from config.
(defn get-external-status!
  [config]
  (let [status {:Config (update-in config [:erc] dissoc :user :password)}]
    (try
      (let [erc-status (get-erc-status! (:erc config))
            db-status (database/get-database-status! (:db config))
            problems (cond
                       (not (:connected-to-DB db-status))                    "Not connected to DB."
                       (not (:connected-to-ERC erc-status))                  "Not connected to ERC."
                       (not (:authenticated-to-ERC erc-status))              "Not authenticated to ERC."
                       (zero? (:LicenseFileCount (:ERC-license erc-status))) "No ERC license found."
                       (:Expired (:ERC-license erc-status))                  "ERC License expired."
                       (not (:Vision (:ERC-license erc-status)))             "Vision not supported by ERC license."
                       (not (:modelid (:ERC-active-model erc-status)))       "No active model."
                       :else                                                 "None")]
        (merge status
          {:ERC      erc-status 
           :DB       db-status
           :Problems problems
           :All-OK   (= "None" problems)}))
      (catch Exception e
        (merge status
          {:Problems [(exception->str e)]
           :All-OK   false})))))

(let [stop-loop (atom false)]
  (defn stop-loop? []  @stop-loop)
  (defn stop-loop! ([v] (reset! stop-loop v))
                   ([]  (stop-loop! true))))


(defn- loop!
  [run-jetty? config]
  (println "in loop! !!!!") 
  (future
    (let [sleep-duration (:status-recheck-frequency (:vision config))
          ^Server jetty-server (when run-jetty?
                                 (info "Starting jetty server ...")
                                 (run-jetty (get-vision-routes-var)
                                   {:port  (:port (:vision config))
                                    :join? false}))]
      (try
        (println "this is jetty server " jetty-server) 
        (let [external-status (get-external-status! config)]
          (update-status! merge external-status)
          (when (:All-OK external-status)
            (update-server-status! "Run initiated")
            (await (run-vision-server! config))))
        
        (while (not (stop-loop?))
          (Thread/sleep sleep-duration)
          (let [current-status  (get-status)
                external-status (get-external-status! config)]
            (update-status! merge external-status)
            (if (:All-OK external-status)
              (do
                (when-not (server-running?)
                  (warn (str "Server coming back online from: " (:Problems current-status)))
                  (update-server-status! "Run initiated")
                  (await (run-vision-server! config))));the await function takes a agent and blocks a thread till the agents completes
              (do
                (when (server-running?)
                  (error (str "Server going down due to: " (:Problems external-status)))
                  (update-server-status! "Stop initiated")
                  (await (stop-vision-server!)))))))
        
        (info "Loop end!")
        
        (when (server-running?)
          (update-server-status! "Stop initiated")
          (await (stop-vision-server!)))
        (info "Exiting loop")
        
        (catch Exception e
          (error "Loop ending due to exception: " e))
        
        (finally
          (when run-jetty?
            (info "Shutting down jetty server ...")
            (.stop jetty-server)))))))

(defn load-config!
  []
  {:db     (load-db-config!)
   :erc    (load-erc-config!)
   :vision (load-vision-config!)})

(defn vision-server-loop!
  ([run-jetty?]
    (println "hello there" run-jetty?)
    (stop-loop! false);changes an atom called stop-loop to false
    (loop! run-jetty? (load-config!)))
  ([]
    (vision-server-loop! true)))

(defn- -main [& args]
  (vision-server-loop!))


(comment
  (let [#^org.apache.log4j.Level debug-level org.apache.log4j.Level/DEBUG
        ekahau-logger (org.apache.log4j.Logger/getLogger "ekahau")]
    (.setLevel ekahau-logger debug-level))
  (declare s *t* *tv*)
  (def s ekahau.vision.server/*server-agent*)
  (defn pe [kw e] (do (ekahau.vision.database/put-entity! (:db @s) kw e) nil))
  (defn c [kw] (ekahau.vision.database/get-entities-count (:db @s) kw))
  (defn ge ([kw] (ge kw {})) ([kw opts] (ekahau.vision.database/get-entities (:db @s) kw opts)))
  (defn gel ([kw] (gel kw 0)) ([kw n] (ge kw {:order-by [[[:id] -1]] :limit n})))
  (defn se ([kw sp] (se kw sp {})) ([kw sp opts] (ekahau.vision.database/search-entities (:db @s) kw sp opts)))
  (defn sc [kw sp] (ekahau.vision.database/search-entities-count (:db @s) kw sp))
  (defn sel ([kw sp] (sel kw sp 0)) ([kw sp n] (se kw sp {:order-by [[[:id] -1]] :limit n})))
  (defn geid [kw id] (ekahau.vision.database/get-entity-by-id (:db @s) kw id))
  (defn deid [kw id] (do (ekahau.vision.database/delete-entity! (:db @s) kw id) nil))
  (defn de [kw] (do (ekahau.vision.database/delete-all-entities! (:db @s) kw) nil))
  (ekahau.test-ekahau/repl-test-bindings!)
  (defn t [] (clojure.test/run-tests *t*))
  (defn tv [] (clojure.test/test-var (intern *t* *tv*)))
  (defmacro with-erc [& body]
    `(ekahau.engine.connection/with-engine-connection (:erc-connection (:engine @s))
       ~@body))
  (set! *print-length* 100)
  (set! *print-level* 20)
  
  #_(def s (atom
             {:db (ekahau.vision.database/create-vision-database!
                    (ekahau.vision.properties/load-db-config!))
              :engine {:erc-connection (ekahau.engine.connection/create-engine-connection
                                         (ekahau.vision.properties/load-erc-config!))}}))
  
  (ekahau.vision.server/vision-server-loop!)
  
  (ekahau.vision.server/reset-vision-routes!)
  
  (ekahau.vision.server/stop-loop!)
  
  (.isOpen (:eve-stream (:event-tracker (:event-manager (:engine @s)))))
  (.getModelId (.getActiveModel (:pos-engine (:loc-tracker (:location-manager (:engine @s))))))
  
  (with-erc (ekahau.engine.cmd/get-user!))
  
  (with-erc
    (binding [ekahau.engine.stream-connection/*request-emsg-seq*
              (partial ekahau.engine.stream-connection/request-emsg-seq
                (:configuration (:engine @s)))]
      (ekahau.vision.par-history-service/update-and-get-par-history!
        (:db @s) (:par-historian (:engine @s))
        ["30"] #{"Asset" "Person"} 0 (System/currentTimeMillis))))
  
  (dotimes [_ 10] (binding [clojure.pprint/pprint (constantly nil)] (t)))
  
  (with-erc
    (doseq [a (ekahau.vision.database/get-entities (:db @s) :assets)]
      (println (:tag-id a))
      (let [engineid (-> (ekahau.engine.cmd/epe-get-tags {:tagid [(:tag-id a)]}) first :properties :assetid)]
        (ekahau.vision.database/put-entity! (:db @s) :assets (assoc a :engine-asset-id engineid)))))
  
  (keys (-> @s :engine :location-manager :par-manager deref :snapshot))
  
  (def f
    (future
      (loop [maxlen 0 sumlen 0 steps 0]
        (Thread/sleep 1000)
        (let [len (.size (:loc-queue (:loc-tracker (:location-manager (:engine @s)))))]
          (when (> len maxlen) (println "Maxlen changed: " len))
          (when (and (not= steps 0) (= 0 (rem steps 60)))
            (println "currlen=" len "maxlen=" maxlen "avglen=" (float (/ sumlen steps))))
          (recur (max len maxlen) (+ sumlen len) (inc steps))))))
  
  (with-erc
    (doseq [t (range 1 10001)]
      (println t)
      (let [id (ekahau.UID/gen)]
        (ekahau.vision.database/put-entity! (:db @s) :assets {:id id :properties [] :tag-id (str t)
                                                              :asset-type-id (if (even? t) "Person" "Asset")})
        (ekahau.vision.routes.asset/bind-tag-to-asset! (:db @s) (str t) id)))))


