(ns ekahau.engine.connection
  (:use
    [clojure.contrib.logging :only [debug]]
    [clojure.contrib.trace :only [trace]]
    [ekahau.engine.emsg :only [make-emsg-array]]
    [ekahau.engine.path :only [epe-path]])
  (:import
    [com.ekahau.common.sdk EMsg EConnection EException]))

(def *engine-connection*)
(def *create-engine-connection*)

(defn create-unauthenticated-engine-connection
  ([host port]
    (create-unauthenticated-engine-connection {:host host :port port}))
  ([{:keys [host port] :as opts}]
    (EConnection. host port)))

(defn create-engine-connection
  ([host port username password]
    (create-engine-connection {:host host :port port :user username :password password}))
  ([{:keys [host port user password] :or {host "localhost" port 8550 user "admin" password "admin"}}]
    (doto (EConnection. host port)
      (.setUserCredentials user password))))

(defmacro with-engine-connection
  [connection & body]
  `(binding [*engine-connection* ~connection]
     ~@body))

(defmacro with-create-engine-connection
  [configuration & body]
  `(binding [*create-engine-connection* (partial create-engine-connection (:host ~configuration) (:port ~configuration))]
     ~@body))

(defn epe-call-raw
  [path & params]
  (try
    (. ^EConnection *engine-connection* call ^String (epe-path path)
      ^"[Lcom.ekahau.common.sdk.EMsg;" (make-emsg-array params))
    (catch EException e
      (debug "Error calling EPE" e)
      (throw e))))

(defn epe-call-raw-binary
  [path & params]
  (try
    (. ^EConnection *engine-connection* call ^String (epe-path path)
      ^"[Lcom.ekahau.common.sdk.EMsg;" (make-emsg-array params) true)
    (catch EException e
      (debug "Error calling EPE" e)
      (throw e))))
