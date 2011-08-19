(ns ekahau.simulation.epe
  (:use
    clojure.set
    [clojure.xml :only (parse)]
    [clojure.contrib.pprint :only (pprint)]
    [clojure.contrib.prxml :only (prxml)]
    [clojure.contrib.duck-streams :only (reader read-lines)]
    [clojure.contrib.seq-utils :only (find-first rand-elt)]
    [clojure.contrib.str-utils :only (str-join)]
    compojure.core
    [compojure.route :only [not-found]]
    [ring.util.servlet :only [servlet]]
    [ekahau.xml :only (to-xml-str xml-element-to-str)]
    ekahau.math
    [ekahau.util :only (create-ids enumerate-names get-image-dimensions long-to-mac)])
  (:import
    [java.io OutputStream]
    [javax.servlet ServletOutputStream]
    [javax.servlet.http HttpServletRequest HttpServletResponse HttpServlet]
    [javax.imageio ImageIO]
    [java.net URL]
    [org.mortbay.jetty Server]
    [org.mortbay.jetty.servlet Context ServletHolder]
    [org.mortbay.jetty.bio SocketConnector]))

(defstruct model :info :maps :zones :position-tags :assets :tag-positions)
(defstruct model-info :id)
(defstruct model-map :id :name :scale :image-filename)
(defstruct model-zone :id :name :map-id :polygon)
(defstruct position-tag :tag-id :mac :name)
(defstruct asset :id :tag-id :name)
(defstruct tag-position :tag-id :timestamp :model-id :map-id :x :y :zone-id)

(defn- create-tags
  [count first-mac]
  (let [ids (create-ids count first-mac)
        macs (map long-to-mac ids)
        names (enumerate-names "Tag")]
    (set (map (partial struct position-tag) ids macs names))))

(defn- add-assets
  [model count]
  (let [tags (create-tags count 0x10000000)
        ids (create-ids count 1)
        assets (set (map (partial struct asset) ids (map :tag-id tags) (enumerate-names "Asset")))]
    (assoc model :position-tags tags :assets assets)))

(defn- create-random-position
  [model]
  (let [random-map (rand-elt (vec (:maps model)))
        [width height] (get-image-dimensions (:image-filename random-map))]
    {:map-id (:id random-map) :x (rand-int width) :y (rand-int height)}))

(defn- assign-random-tag-positions
  [model]
  (let [now (System/currentTimeMillis)
        tag-ids (map :tag-id (:assets model))
        model-id (-> model :info :id)
        random-positions (repeatedly create-random-position)]
    (assoc model
      :tag-positions
      (set
        (map
          merge
          (map (partial struct tag-position) tag-ids (repeat now) (repeat model-id))
          (repeatedly (partial create-random-position model)))))))

(defn- create-base-model
  []
  (struct model
    ; Model Info
    (struct model-info 0)
    ; Maps
    #{
      (struct model-map 0 "Floor 1" 80.0 "simdata/mapimage.png")
      }
    ; Zones
    #{
      (struct model-zone 1 "Zone 1" 0 [[100, 100] [200, 100] [200, 200] [100, 200]])
      (struct model-zone 2 "Zone 2" 0 [[300, 300] [700, 300] [700, 600] [300, 600]])
      (struct model-zone 3 "Zone 3" 0 [[800, 10] [1100, 10] [1100, 750] [800, 750]])
      }
    ; Tags
    #{}
    ; Assets
    #{}
    ; Tag Positions
    #{}))

(def *model-ref* (ref (create-base-model)))

(defn create-route-plan
  [tag-id route speed]
  {:tag-id tag-id :route route :speed speed})

(defn- create-random-route
  []
  (repeatedly #(vector (rand-int 1181) (rand-int 788))))

(defn- create-route-points
  [asset-id start-time map-id points speed]
  (lazy-seq
    (let [[x y :as point] (first points)
          distance (get-points-distance point (second points))
          duration (* 1000 (/ distance speed))]
      (cons
        {:asset-id asset-id :timestamp start-time :map-id map-id :x x :y y}
        (create-route-points asset-id (+ start-time duration) map-id (rest points) speed)))))

(def *plans-ref* (ref []))

(def *stream-id-counter* (ref 0))
(def *tag-streams* (ref {})) ; List of updates per stream id

(defn next-position
  [current distance route]
  (if (seq route)
    (let [v (vector-minus (first route) current)
          vl (vector-length v)]
      (if (< vl distance)
        (next-position (first route) (- distance vl) (rest route))
        [(vector-plus current (vector-to-length v distance)) route]))
    [current (seq route)]))

(defn- select-by
  [k v xset]
  (select #(-> % k (= v)) xset))

(defn- get-tag-point
  [tag]
  [(:x tag) (:y tag)])

(defn- replace-relations
  [xset old new]
  (union (difference xset old) new))

(defstruct plan-result :model :plan)

(defn implement-plan
  [model plan time]
  (let [{:keys [tag-id route speed]} plan
        tag-positions (select-by :tag-id tag-id (:tag-positions model))]
    (if-let [tag-position (first tag-positions)]
      (let [distance (* speed (/ (- time (:timestamp tag-position)) 1000))
            [[x y] new-route] (next-position (get-tag-point tag-position) distance route)]
        (struct plan-result
          (assoc-in model [:tag-positions]
            (replace-relations
              (:tag-positions model)
              tag-positions
              #{(assoc tag-position :x x :y y :timestamp time)}))
          (assoc plan :route new-route)))
      (struct plan-result model plan))))

(defn- format-timestamp
  [t]
  (str t))

(defn- get-map-name-by-id
  [id]
  (if-let [result (first (select #(-> % :id (= id)) (:maps @*model-ref*)))]
    (:name result)
    nil))

(defn- get-zone-name-by-id
  [id]
  (if-let [result (first (select #(-> % :id (= id)) (:zones @*model-ref*)))]
    (:name result)
    nil))

(defn- model-map-to-xml
  [m]
  [:MAP
   [:mapid (:id m)]
   [:mapname (:name m)]
   [:zonecount 0]
   [:scale (:scale m)]
   [:scaleunit "pixels/meter"]
   ])

(defn model-zone-to-xml
  [z]
  [:ZONE
   [:zoneid (:id z)]
   [:mapid (:map-id z)]
   [:zonename (:name z)]
   [:polygon
    (str-join ";"
      (map
        (fn [point]
          (str-join "," point))
        (:polygon z)))
    ]
   ])


(defn- position-tag-to-xml
  [t]
  [:TAG
   [:tagid (:tag-id t)]
   [:mac (:mac t)]
   ])

(defn- tag-position-to-xml
  [p]
  [[:postime (:timestamp p)]
   [:postimestamp (format-timestamp (:timestamp p))]
   [:posmodelid (:model-id p)]
   [:posmapid (:map-id p)]
   [:posmapname (get-map-name-by-id (:map-id p))]
   [:posx (:x p)]
   [:posy (:y p)]])

(defn- model-browse
  []
  (to-xml-str
    (into
      [:response [:MODEL]]
      (concat
        (map model-map-to-xml (:maps @*model-ref*))
        (map model-zone-to-xml (:zones @*model-ref*))))))

(defn- get-tag-list
  []
  (let [j (dosync (join (:position-tags @*model-ref*) (:tag-positions @*model-ref*)))]
    (to-xml-str
      (into
        [:response]
        (map #(into (position-tag-to-xml %) (tag-position-to-xml %)) j)))))

(defn- get-tag-group-list
  []
  (to-xml-str
    [:response]))

(defn- parse-post-content
  [f]
  (try
    (clojure.xml/parse f)
    (catch org.xml.sax.SAXParseException _ nil)))

(defn- get-model-id
  []
  (-> *model-ref* deref :info :id))

(defn- route-point-to-xml
  [p]
  [:ROUTEPOINT
   [:tagid (:asset-id p)]
   [:routetime (long (:timestamp p))]
   [:timestamp (System/currentTimeMillis)]
   [:mapid (:map-id p)]
   [:x (:x p)]
   [:y (:y p)]])

(defn- create-route-points-xml
  [asset-id map-id route-start-time route-end-time]
  (map
    route-point-to-xml
    (take-while
      #(< (:timestamp %) route-end-time)
      (create-route-points asset-id route-start-time map-id (ekahau.simulation.epe/create-random-route) 10))))

(defn- with-tag
  [kw]
  (fn [e]
    (-> e :tag (= kw))))

(defn parse-routehistorybrowse-request
  [m]
  (let [object-element (find-first (with-tag :OBJECT) (:content m))
        tag-ids (:content (find-first (with-tag :tagid) (:content object-element)))
        [start-time] (:content (find-first (with-tag :since) (:content object-element)))
        [end-time] (:content (find-first (with-tag :until) (:content object-element)))]
    {:tag-ids (map #(Long/parseLong %) (.split ^String (first tag-ids) ",")),
     :time-interval [(Long/parseLong start-time) (Long/parseLong end-time)]}))

; START: defservlet
(defroutes epe-simulator-app
  (GET "/epe/mod/modelgetactive" {:as request}
    (to-xml-str
      [:response
       [:MODEL
        [:modelid (get-model-id)]
        ]
       ]))
  
  (GET "/epe/mod/modelbrowse" {:as request}
    (model-browse))
  
  (POST "/epe/mod/modelbrowse" {:as request}
    (model-browse))
  
  (POST "/epe/pos/taglist" {:as request}
    (get-tag-list))
  
  (GET "/epe/cfg/taggrouplist" {:as request}
    (get-tag-group-list))
  
  (POST "/epe/mod/mapview" {:as request}
    (java.io.FileInputStream. "simdata/mapimage.png"))
  
  (ANY "/epe/route/routehistorybrowse" {:as request}
    (let [body-xml (parse (:body request))
          {:keys [tag-ids time-interval]} (parse-routehistorybrowse-request body-xml)]
      
      (to-xml-str
        (let [route (create-random-route)
              now (System/currentTimeMillis)
              [route-start-time route-end-time] time-interval]
          (into [:response]
            (map #(create-route-points-xml % 0 route-start-time route-end-time) tag-ids))))))
  
  (not-found ""))

; END: defservlet

(def scan-reason-periodic {:code 3 :name "Periodic"})

(def scan-reasons #{scan-reason-periodic})

(defn- create-tag-message
  [tag]
  [:TAG
   [:tagid (:tag-id tag)]
   [:mac (:mac tag)]
   [:posx (Math/round (double (:x tag)))]
   [:posy (Math/round (double (:y tag)))]
   [:posmodelid (:model-id tag)]
   [:posmapid (:map-id tag)]
   [:poszoneid (:zone-id tag)]
   [:posmapname (get-map-name-by-id (:map-id tag))]
   [:poszonename (get-zone-name-by-id (:zone-id tag))]
   [:posquality 100]
   [:posreason (:code scan-reason-periodic)]
   [:postime (:timestamp tag)]
   [:postimestamp  (format-timestamp (:timestamp tag))] ; "2009-08-26 15:52:57+0300"
   [:batterylevel 100]
   [:poscounter 0]
   [:posfilter "PASS"]
   [:charging "FALSE"]
   [:name (:name tag)]
   [:type "t301b"]
   ])

(defn- create-tag-stream-response
  [tag]
  [:response (create-tag-message tag)])

(defn- create-ascii-message-key-value-pair
  [k v]
  (str (name k) "=" (count (str v)) ":" v))

(defn- create-ascii-message
  [m]
  (str-join " "
    (concat
      [(name (first m))]
      (map
        #(create-ascii-message-key-value-pair (first %) (second %))
        (rest m)))))

(defn- produce-chunks
  [id ^ServletOutputStream out]
  (doseq [num (iterate inc 1)]
    (dosync
      (let [stream (get @*tag-streams* id)]
        (alter *tag-streams* assoc id [])
        (doseq [u stream]
          (.print out ^String (create-ascii-message (create-tag-message u)))
          (.print out "\r\n"))))
    (.flush out)
    (Thread/sleep 1000)))

(defn- create-tagstream-servlet
  []
  (proxy [HttpServlet] []
    (service [^HttpServletRequest request ^HttpServletResponse response]
      (doto response
        (.setStatus 200)
        (.addHeader "Transfer-Encoding" "chunked"))
      (let [stream-id (dosync (alter *stream-id-counter* inc))]
        (try
          (produce-chunks stream-id (.getOutputStream response))
          (catch java.io.IOException ex
            (dosync (alter *tag-streams* dissoc stream-id))))))))

(defn- print-request
  [request]
  (println "REQUEST")
  (apply println (map request [:request-method :uri :query-string]))
  (when-let [body (:body request)]
    (println (read-lines body)))
  (println))

(defn- print-response
  [response]
  (println "RESPONSE")
  (println response)
  (println))

(defn- with-logging
  [handler]
  (fn [request]
    (print-request request)
    (let [response (handler request)]
      (print-response response)
      response)))

(defn- add-updates
  [streams updates]
  (reduce
    #(assoc %1 (first %2) (into (second %2) updates))
    {}
    streams))

(defn- implement-plans
  [model plans]
  (let [current-time (System/currentTimeMillis)]
    (reduce
      (fn [[model plans] current-plan]
        (let [{result-model :model result-plan :plan} (implement-plan model current-plan current-time)]
          [result-model (conj plans result-plan)]))
      [model []]
      plans)))

(defn- simulate
  [model-ref plans-ref tag-streams-ref]
  (let [foo (map #(create-route-plan % (create-random-route) 10) (map :tag-id (:assets @model-ref)))]
    (dosync (ref-set plans-ref foo)))
  
  (while true
    (dosync
      (let [[the-result-model the-result-plans] (implement-plans @model-ref @plans-ref)]
        (ref-set model-ref the-result-model)
        (ref-set plans-ref the-result-plans)
        (alter tag-streams-ref add-updates (join (:position-tags @model-ref) (:tag-positions @model-ref)))))
    (Thread/sleep 1000)))

;;;;;;;;;;;;;;;;;;;;;

(defn get-host-and-path
  "Splits a path or URL into its hostname and path."
  [url-or-path]
  (if (re-find #"^[a-z+.-]+://" url-or-path)
    (let [url (URL. url-or-path)]
      [(.getHost url) (.getPath url)])
    [nil url-or-path]))

(defn server-with-options
  "Create a new server using the supplied function, options and servlets."
  [creator options servlets]
  (if (map? options)
    (creator options servlets)
    (creator {} (cons options servlets))))

(defn get-context
  "Get a Context instance for a server and hostname."
  ([server]
    (get-context server nil))
  ([server host]
    (let [context (Context. server "/" Context/SESSIONS)]
      (if host
        (doto context (.setVirtualHosts (into-array [host])))
        context))))

(defn add-servlet!
  [server url-or-path servlet]
  (prn (class servlet))
  (let [[host path] (get-host-and-path url-or-path)
        context     (get-context server host)
        holder      (if (instance? ServletHolder servlet)
                      servlet
                      (ServletHolder. servlet))]
    (.addServlet context holder path)))

(defn- create-server
  "Construct a Jetty Server instance."
  [options servlets]
  (let [connector (doto (SocketConnector.)
                    (.setPort (options :port 80))
                    (.setHost (options :host)))
        server    (doto (Server.)
                    (.addConnector connector))
        servlets  (partition 2 servlets)]
    (doseq [[url-or-path servlet] servlets]
      (add-servlet! server url-or-path servlet))
    server))

(defn jetty-server
  "Create a new Jetty HTTP server with the supplied options and servlets."
  [options? & servlets]
  (server-with-options create-server options? servlets))

(defmacro defserver
  "Shortcut for (def name (http-server args))"
  [name & args]
  `(def ~name (jetty-server ~@args)))

(defn start "Start a HTTP server."
  [server]
  (.start server))

(defn stop  "Stop a HTTP server."
  [server]
  (.stop server))

;;;;;;;;;;;;;;;;;;;;;

; START: defserver
(defserver epe-sim-server
  {:port 8550}
  "/epe/pos/tagstream" (create-tagstream-servlet)
  "/*" (servlet epe-simulator-app))
; END: defserver

(defn- start-server!
  []
  (doto (Thread. ^Runnable (partial simulate *model-ref* *plans-ref* *tag-streams*))
    (.start))
  (start epe-sim-server))
