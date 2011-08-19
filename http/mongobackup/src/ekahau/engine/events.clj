(ns ekahau.engine.events
  (:require
    [ekahau.vision.database :as database])
  (:use
    [ekahau.engine.path :only [epe-path]]
    [ekahau.engine.emsg :only [unpack-eresponse]]
    [ekahau.queue :only [seq-from-blocking-queue]]
    [clojure.contrib.logging :only [debug info warn error]])
  (:import
    [java.util.concurrent BlockingQueue LinkedBlockingQueue]
    [com.ekahau.common.sdk EResponse IStreamListener EStreamConnection]))

(defn- parse-event
  [eresp]
  (unpack-eresponse eresp))

(defn- create-queueing-events-listener
  [^BlockingQueue queue]
  (proxy [IStreamListener] []
    (newResponse [^EResponse response]
      (doseq [emsg (parse-event response)]
        (when-not (= "QUERYALIVE" (:type emsg))
          (.put queue emsg))))
    (connectionClosed [^Exception reason]
      (when-not (= "client called close" (.getMessage reason))
        (error "Event's IStreamListener.connectionClosed called" reason))
      (Thread/interrupted) ;; hack needed coz ERC interrupts the thread, this clears the flag
      (.put queue ::events-terminator))))

(defn- add-request-parameters!
  [^EStreamConnection estream-connection request-parameters]
  (doseq [[k v] request-parameters]
    (doseq [elem (if (coll? v) v [v])]
      (.addRequestParameter estream-connection (name k) (str elem)))))

(defn create-event-tracking-engine
  [{:keys [host port user password request-params] :as erc-config}]
  (let [queue (LinkedBlockingQueue.)
        listener (create-queueing-events-listener queue)]
    {:erc-config erc-config
     :eve-queue  queue
     :eve-stream (doto (EStreamConnection.)
                   (.setUserCredentials user password)
                   (add-request-parameters! request-params)
                   (.addListener listener))}))

(defn start-tracking-events!
  [{:keys [erc-config eve-queue eve-stream]}]
  (.connect ^EStreamConnection eve-stream
    (:host erc-config) (:port erc-config) (epe-path "eve/eventstream")))

(defn stop-tracking-events!
  [{:keys [eve-queue eve-stream]}]
  (.close ^EStreamConnection eve-stream))

(defn event-seq
  [{:keys [eve-queue eve-stream]}]
  (seq-from-blocking-queue eve-queue #(= ::events-terminator %)))
