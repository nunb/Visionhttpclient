(ns ekahau.vision.routes.session
  (:require
    [ring.middleware.session memory store]))

(defonce *vision-session-params* {:store (ring.middleware.session.memory/memory-store)
                                  :cookie-name "vision-session"})

(defn- get-store [] (:store *vision-session-params*))

(defn- get-session-key
  [request]
  (get-in request [:cookies (:cookie-name *vision-session-params*) :value]))

(defn delete-session!
  [request]
  (when-let [session-key (get-session-key request)]
    (ring.middleware.session.store/delete-session (get-store) session-key)))
