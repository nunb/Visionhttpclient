(ns ekahau.engine.stream-connection
  (:require
    [ekahau.queue])
  (:use
    [clojure.contrib.logging :only [info]]
    [clojure.contrib.trace :only [trace]]
    [ekahau.engine.emsg :only [unpack-eresponse]]
    [ekahau.engine.path :only [epe-path]])
  (:import
    [com.ekahau.common.sdk IStreamListener EStreamConnection]
    [java.util.concurrent BlockingQueue LinkedBlockingQueue]))

(defn- add-request-parameters!
  [^EStreamConnection estream-connection request-parameters]
  (doseq [[k v] request-parameters]
    (doseq [elem (if (coll? v) v [v])]
      (.addRequestParameter estream-connection (name k) (str elem)))))

(defn- create-streaming-engine-connection
  [username password request-parameters]
  (doto (EStreamConnection.)
    (.setUserCredentials username password)
    (add-request-parameters! request-parameters)))

(defn- create-queueing-stream-listener
  [^BlockingQueue queue]
  (proxy [IStreamListener] []
    (newResponse
      [response]
      (.put queue response))
    (connectionClosed
      [reason]
      (info "IStreamListener connectionClosed" reason)
      (.put queue :terminator))))

(defn create-streaming-engine-connection-agent
  [{:keys [host port user password]} path request-parameters]
  (agent {:host host
          :port port
          :path path
          :connection (create-streaming-engine-connection user password request-parameters)}))

(defn connect-streaming-engine-connection
  [connection-agent]
  (send connection-agent
    (fn [s]
      (.connect ^EStreamConnection (:connection s)
        (:host s) (:port s) (epe-path (:path s)))
      s)))

(defn close-streaming-engine-connection
  [connection-agent]
  (send connection-agent
    (fn [s]
      (.close ^EStreamConnection (:connection s))
      s)))

(defn- add-streaming-connection-listener
  [state listener]
  (.addListener ^EStreamConnection (:connection state) listener)
  state)

(defn- remove-streaming-connection-listener
  [state listener]
  (.removeListener ^EStreamConnection (:connection state) listener)
  state)

(defn- epe-stream-to-queue
  [{:keys [host port user password] :or {host "localhost" port 8550 user "admin" password "admin"}} path params]
  (let [queue (LinkedBlockingQueue. 32)]
    (doto ^EStreamConnection (create-streaming-engine-connection user password (merge params {:streaming "true"}))
      (.addListener (create-queueing-stream-listener queue))
      (.connect host port (epe-path path)))
    queue))

(defn request-emsg-seq
  [engine-configuration path params]
  (->> (epe-stream-to-queue engine-configuration path params)
    (ekahau.queue/seq-from-blocking-queue)
    (mapcat unpack-eresponse)))

;; Version with bound engine-configuration
(declare *request-emsg-seq*)

(defn connect-new-queueing-listener
  [connection-agent]
  (let [queue-atom (ekahau.queue/create-queue-atom)
        listener (create-queueing-stream-listener (:queue @queue-atom))]
    (send connection-agent add-streaming-connection-listener listener)
    {:connection-agent connection-agent
     :queue-atom queue-atom
     :listener listener}))

(defn open-emsg-seq
  [{queue-atom :queue-atom}]
  (->> (ekahau.queue/open-queue-as-seq! queue-atom)
    (mapcat unpack-eresponse)
    (remove #(= "QUERYALIVE" (:type %)))))

(defn disconnect-queue-listener
  [{:keys [connection-agent listener queue-atom]}]
  (send connection-agent
    (fn [state]
      (remove-streaming-connection-listener state listener)
      (ekahau.queue/close-queue! queue-atom)
      state)))

(comment 
  "Queue listener example"
  (let [streaming-connection (create-streaming-engine-connection-agent {:host "white" :user "ekahau" :password "ekahau"} "eve/eventstream" {})
        listener (connect-new-queueing-listener streaming-connection)
        result (future (open-emsg-seq listener))]
    (connect-streaming-engine-connection streaming-connection)
    (await streaming-connection)
    (Thread/sleep 10000)
    (disconnect-queue-listener listener)
    (close-streaming-engine-connection streaming-connection)
    (await streaming-connection)
    @result))
