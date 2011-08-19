(ns ekahau.vision.routes.event
  (:require
    [ekahau.UID :as uid]
    [ekahau.vision.event-service :as es]
    [ekahau.vision.engine-service :as engine-service])
  (:use
    compojure.core
    ekahau.vision.routes.helpers
    [clojure.contrib.json :only [read-json]]
    [clojure.contrib.logging :only [error]]
    [ekahau
     [string :only [parse-id parse-int parse-number]]])
  (:import java.util.Date))

(defn- get-events-response
  [request db]
  (with-useful-bindings request
    {:skip skip :limit limit}
    (to-json (es/get-events db :skip skip :limit limit))))

(defn- get-event-response
  [request db]
  (with-useful-bindings request
    {:id event-id}
    (to-json (es/get-event-by-id db event-id))))

(defn- get-nots-alerts-read-params
  [request]
  (let [read-to-boolean-map {"read" true "unread" false}]
    [(get read-to-boolean-map
       (-> request :params :notifications))
     (get read-to-boolean-map
       (-> request :params :alerts))]))

(defn- get-user-mailbox-response
  [request db]
  (with-useful-bindings request
    {:user user :skip skip :limit limit}
    (let [last-max-msg-id (or (-> request :params :lastMaxMsgId parse-id)
                            uid/MIN-UID)
          [nots-read? alerts-read?] (get-nots-alerts-read-params request)]
      (to-json (es/get-user-mailbox db (:uid user)
                 last-max-msg-id false
                 nots-read? alerts-read?
                 :skip skip :limit limit)))))

(defn- get-last-few-user-mailbox-response
  [request db]
  (with-useful-bindings request
    {:user user}
    (let [n (or (-> request :params :N parse-int) 0)
          [nots-read? alerts-read?] (get-nots-alerts-read-params request)]
      (to-json (es/get-last-few-user-mailbox db (:uid user) n
                 nots-read? alerts-read?)))))

(defn- comment-on-event-response!
  [request db]
  (with-useful-bindings request
    {:id event-id :timestamp timestamp
     :json-body comment :user user}
    (try
      (es/comment-on-event! db event-id {:user user
                                         :time timestamp
                                         :text comment})
      (to-json {:result "OK"})
      (catch Exception e
        (error "Error while adding comment to event " e)
        (create-bad-request-response request (.getMessage e) to-json)))))

(defn- close-event-response!
  [request db]
  (with-useful-bindings request
    {:id event-id :timestamp timestamp
     :json-body closing-remark :user user}
    (try
      (es/close-event! db event-id {:user user
                                    :time timestamp
                                    :text closing-remark})
      (to-json {:result "OK"})
      (catch Exception e
        (error "Error while closing event " e)
        (create-bad-request-response request (.getMessage e) to-json)))))

(defn- accept-event-response!
  [request db]
  (with-useful-bindings request
    {:id event-id :timestamp timestamp :user user}
    (try
      (es/accept-event! db event-id {:user user
                                     :time timestamp})
      (to-json {:result "OK"})
      (catch Exception e
        (error "Error while accepting event " e)
        (create-bad-request-response request (.getMessage e) to-json)))))

(defn- decline-event-response!
  [request db]
  (with-useful-bindings request
    {:id event-id :timestamp timestamp :user user}
    (try
      (es/decline-event! db event-id {:user user
                                      :time timestamp})
      (to-json {:result "OK"})
      (catch Exception e
        (error "Error while declining event " e)
        (create-bad-request-response request (.getMessage e) to-json)))))

(defn- mark-message-read-response!
  [request db]
  (with-useful-bindings request
    {:id msg-id :user user}
    (try
      (es/mark-msg-read! db (:uid user) msg-id)
      (to-json {:result "OK"})
      (catch Exception e
        (error "Error marking message read " e)
        (create-bad-request-response request (.getMessage e) to-json)))))

(defn- mark-message-unread-response!
  [request db]
  (with-useful-bindings request
    {:id msg-id :user user}
    (try
      (es/mark-msg-unread! db (:uid user) msg-id)
      (to-json {:result "OK"})
      (catch Exception e
        (error "Error marking message unread " e)
        (create-bad-request-response request (.getMessage e) to-json)))))

(defn- get-mailbox-latest
  [request db]
  (with-useful-bindings request
    {:user user :limit limit}
    (let [[nots-read? alerts-read?] (get-nots-alerts-read-params request)]
      (to-json (es/get-user-mailbox-latest db (:uid user)
                 {:nots-read? nots-read? :alerts-read? alerts-read?
                  :limit limit})))))

(defn- get-mailbox-next
  [request db]
  (with-useful-bindings request
    {:user user :limit limit}
    (let [newer-than (-> request :params :newerThan parse-id)
          [nots-read? alerts-read?] (get-nots-alerts-read-params request)]
      (to-json (es/get-user-mailbox-next db (:uid user)
                 {:nots-read? nots-read? :alerts-read? alerts-read?
                  :newer-than newer-than :limit limit})))))

(defn- get-mailbox-previous
  [request db]
  (with-useful-bindings request
    {:user user :limit limit}
    (let [older-than (-> request :params :olderThan parse-id)
          [nots-read? alerts-read?] (get-nots-alerts-read-params request)]
      (to-json (es/get-user-mailbox-prev db (:uid user)
                 {:nots-read? nots-read? :alerts-read? alerts-read?
                  :older-than older-than :limit limit})))))

(defn create-event-routes
  [db]
  (-> (routes
        (GET "/events" {:as request}
          (get-events-response request db))
        (GET "/events/:id" {:as request}
          (get-event-response request db))
        (GET "/mailbox" {:as request}
          (get-user-mailbox-response request db))
        (GET "/mailbox/last" {:as request}
          (get-last-few-user-mailbox-response request db))
        (GET "/mailbox/latest" {:as request}
          (get-mailbox-latest request db))
        (GET "/mailbox/next" {:as request}
          (get-mailbox-next request db))
        (GET "/mailbox/previous" {:as request}
          (get-mailbox-previous request db))
        (POST "/events/:id/comment" {:as request}
          (comment-on-event-response! request db))
        (POST "/events/:id/close" {:as request}
          (close-event-response! request db))
        (POST "/events/:id/accept" {:as request}
          (accept-event-response! request db))
        (POST "/events/:id/decline" {:as request}
          (decline-event-response! request db))
        (PUT "/mailbox/:id/read" {:as request}
          (mark-message-read-response! request db))
        (PUT "/mailbox/:id/unread" {:as request}
          (mark-message-unread-response! request db)))
    (with-json-content-type)))

