(ns ekahau.vision.routes
  (:use
    compojure.core
    ekahau.vision.routes.helpers
    [compojure.handler :only [site]]
    [ring.util.response :only [redirect]]
    [clojure.contrib.logging :only [debug info]]
    [clojure.contrib.trace :only [trace]]
    [ekahau.vision.routes
     [session :only [*vision-session-params*]]
     [address :only [create-address-routes]]
     [asset-import :only [create-asset-import-routes
                          create-create-import-routes]]
     [asset-search :only [create-asset-search-routes]]
     [asset :only [create-asset-routes]]
     [asset-type :only [create-asset-type-routes]]
     [asset-spec :only [create-asset-specification-routes]]
     [building :only [create-building-routes]]
     [event :only [create-event-routes]]
     [event-rule :only [create-event-rule-routes]]
     [event-search :only [create-event-search-routes]]
     [event-report :only [create-event-report-routes]]
     [icon :only [create-icon-routes]]
     [login :only [create-login-routes with-login-check]]
     [map :only [create-map-routes]]
     [report :only [create-report-routes]]
     [tag :only [create-tag-routes]]
     [user :only [create-user-routes]]
     [zone-grouping :only [create-zone-grouping-routes]]
     [debug :only [create-debug-routes]]
     [system :only [create-system-routes]]
     [export :only [create-export-routes]]
     [todolist :only [create-todolist-routes]]
     [site :only [create-site-routes]]
     [par-site-view :only [create-par-site-view-routes]]])
  (:import
    [clout.core CompiledRoute]))

(defn- create-html-js-swf-and-ico-route
  []
  (routes
    (GET "/" []
      (redirect "/index.html"))
    (GET (CompiledRoute. #"(?i)/(.*\.(html|js|swf|ico))" [:file-name] false)
      [file-name]
      (public-file-response file-name))))

(defn- create-file-route
  []
  (GET "/:file-name" [file-name]
    (public-file-response file-name)))

(defn- create-default-resource-not-found-route
  []
  (->
    (ANY "/*" {:as request} (create-resource-not-found-response request))
    (with-xml-content-type)))

(defn- with-system-check
  [handler server-available?]
  (fn [request]
    (if server-available?
      (handler request)
      (create-service-unavailable-response request))))

(defn create-routes
  [db engine server-available?]
  (println "create-routes function is probably called first")
  (->
    (routes
      (routes
        (create-system-routes)
        (create-create-import-routes db)
        (create-debug-routes db engine)
        (create-html-js-swf-and-ico-route))
      (->
        (routes
          (->
            (routes
              (create-login-routes db engine))
            (with-engine-binding engine))
          (->
            (routes
              ; Users
              (create-user-routes db)
              ; Events/Rules/Mailbox
              (create-event-routes db)
              (create-event-rule-routes db)
              (create-event-search-routes db)
              (create-event-report-routes db)
              ; Asset/Asset-types
              (create-asset-routes db)
              (create-asset-type-routes db)
              (create-asset-import-routes db)
              (create-asset-search-routes db)
              (create-asset-specification-routes db)
              ; Tag
              (create-tag-routes db)
              ; TO-DO list
              (create-todolist-routes db)
              ; Site View
              (create-map-routes db)
              (create-address-routes db)
              (create-building-routes db)
              (create-zone-grouping-routes db)
              (create-par-site-view-routes db engine)
              (create-site-routes db)
              ; Reports
              (create-report-routes db)
              ; Export
              (create-export-routes db)
              ; Icons
              (create-icon-routes))
            (with-user-engine-binding)
            (with-login-check)))
        (with-system-check server-available?))
      
      (routes
        (create-file-route)
        (create-default-resource-not-found-route)))
    
    (with-cache-control "no-cache, must-revalidate")
    (with-rest-header-to-method)
    (with-route-logging)
    (site {:session *vision-session-params*})))
