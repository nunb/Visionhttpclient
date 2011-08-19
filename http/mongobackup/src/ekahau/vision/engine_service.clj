(ns ekahau.vision.engine-service
  (:require
    [clojure.contrib.duck-streams :as duck-streams]
    [ekahau.engine]
    [ekahau.engine.cmd]
    [ekahau.engine.connection]
    [ekahau.vision.database :as database])
  (:use
    [clojure.contrib.logging :only (debug error info spy warn)]
    [clojure.contrib.seq-utils :only [indexed]]
    [clojure.contrib.str-utils :only [re-split str-join]]
    [clojure.contrib.trace :only [trace]]
    [ekahau.vision.properties :only [load-erc-config!]]
    [ekahau.predicates :only [substring-pred?]]
    [ekahau.string :only [parse-id parse-double parse-int parse-long parse-boolean]]
    [ekahau.vision.model.asset :only [get-asset-by-engine-asset-id
                                      list-assets-by-asset-type]])
  (:import
    [java.util Date Locale]
    [java.text SimpleDateFormat]
    [com.ekahau.common.sdk EConnection EMsg EResponse EException]))

;; - Maps ;;

(defn cache-image-data!
  "Returns a temporary file holding image data."
  [map-id bytes]
  (let [f (java.io.File/createTempFile (str "image-" map-id "-") ".png")]
    (.deleteOnExit f)
    (duck-streams/copy bytes f)
    f))

(defn- load-map-image
  [modelid mapid]
  (let [^EResponse response (ekahau.engine.connection/epe-call-raw-binary "mod/mapview" {:modelid modelid, :mapid mapid, :format "png"})
        ^"[Lcom.ekahau.common.sdk.EMsg;" messages (.get response)
        ^EMsg m (aget messages 0)]
    (.getBin m "data_b" nil)))

(defn- start-loading-map-images
  [db model-id]
  (info "Start loading map images.")
  (let [engine-connection ekahau.engine.connection/*engine-connection*
        maps (database/get-entities db :maps)
        map-count (count maps)]
    (future
      (ekahau.engine.connection/with-engine-connection engine-connection
        (try
          (doseq [[index m] (indexed maps)]
            (let [map-id (:id m)]
              (info (format "Loading map image %d/%d (id %s)" (inc index) map-count map-id))
              (let [image-bytes (load-map-image model-id map-id)]
                (deliver (:image m) (cache-image-data! map-id image-bytes)))
              (info (format "Loaded map image %d/%d (id %s)" (inc index) map-count map-id))))
          (catch Exception e
            (error "Error loading map images" e))))))
  db)

;; - Icons ;;

(defn- set-icon-for-asset-type
  [asset-type-icons asset-type]
  (assoc asset-type :icon (get asset-type-icons (:name asset-type))))

(defn- parse-asset-type-icons-conf
  []
  (apply hash-map (apply concat (map rest (re-seq #"\"(.*)\".*\"(.*)\"" (slurp "conf/asset-type-icons.conf"))))))

(defn- map-asset-type-icons
  [model]
  (let [asset-type-icons (parse-asset-type-icons-conf)]
    (update-in model [:asset-types] #(set (map (partial set-icon-for-asset-type asset-type-icons) %)))))

(defn- parse-emsgs-of-type
  [epe-model type-name parse-fn]
  (set (map parse-fn (filter #(-> % :type (= type-name)) epe-model))))

(defn- parse-building
  [{{id-str :buildingid name :buildingname} :properties}]
  (struct database/building
    (parse-id id-str)
    name))

(defn- parse-map 
  [{{id-str :mapid building-id-str :buildingid name :mapname scale-str :scale scale-unit :scaleunit} :properties}]
  (struct-map database/model-map
    :id (parse-id id-str)
    :building-id (parse-id building-id-str)
    :name name
    :image (promise)
    :scale (struct database/scale (parse-double scale-str) scale-unit)))

(defn- parse-buildings
  [epe-model]
  (info "Loading buildings...")
  (parse-emsgs-of-type epe-model "BUILDING" parse-building))

(defn parse-maps 
  [epe-model]
  (info "Loading maps...")
  (parse-emsgs-of-type epe-model "MAP" parse-map))

(defn- parse-point
  [^String s]
  (vec (map parse-double (.split s ","))))

(defn- parse-polygon
  [^String s]
  (vec (map parse-point (.split s ";"))))

(defn- parse-zone
  [{{id-str :zoneid name :zonename map-id-str :mapid polygon-str :polygon} :properties}]
  (struct database/zone (parse-id id-str) name
    (struct database/area (parse-id map-id-str) (parse-polygon polygon-str))))

(defn parse-zones
  [epe-model]
  (info "Loading zones...")
  (parse-emsgs-of-type epe-model "ZONE" parse-zone))

(defn- parse-floors
  [epe-model]
  (:floors
    (reduce
      (fn [state emsg]
        (condp = (:type emsg)
          "BUILDING" (assoc state
                       :current-building (parse-building emsg)
                       :order-num 1)
          "MAP" (if (:current-building state)
                  (-> state
                      (update-in [:floors] conj (struct-map database/floor
                                                  :id (-> emsg parse-map :id)
                                                  :building-id (-> state :current-building :id)
                                                  :map-id (-> emsg parse-map :id)
                                                  :order-num (:order-num state)))
                    (update-in [:order-num] inc))
                  state)
          state))
      {:current-building nil :order-num nil :floors #{}}
      epe-model)))

(defn- load-map-and-zones
  [db epe-model]
  (-> db
    (database/put-entities! :buildings (parse-buildings epe-model))
    (database/put-entities! :maps (parse-maps epe-model))
    (database/put-entities! :zones (parse-zones epe-model))
    (database/put-entities! :floors (parse-floors epe-model))))

(defn- get-active-model-id-from-erc
  []
  (get-in (first (ekahau.engine.cmd/epe-get-active-model)) [:properties :modelid]))

(defn get-active-model-id
  [db]
  (:id (first (filter :active (database/get-entities db :models)))))

(defn- make-active-model-inactive
  [db]
  (if-let [id (get-active-model-id db)]
    (database/update-entity! db :models id assoc :active false)
    db))

(defn- insert-active-model
  [db model-id]
  (database/put-entity! db :models (struct-map database/model :id model-id :active true)))

(declare ensure-asset-types-have-engine-asset-groups!)
(declare asset-type-asset-group-sync-agent)
(declare sync-asset-type-group-assets!)

(defn- list-engine-asset-ids-in-engine
  []
  (->> (ekahau.engine.cmd/list-assets)
    (map #(-> % :properties :assetid parse-id))))

(defn- ensure-asset-engine-assets-exist!
  [db]
  (info "Ensuring engine-assets exist ...")
  (let [exists-in-engine? (set (list-engine-asset-ids-in-engine))]
    (reduce
      (fn [db {:keys [engine-asset-id] :as asset}]
        (if (or (nil? engine-asset-id) (exists-in-engine? engine-asset-id))
          db
          (do
            (warn (format "Engine asset unexpectedly disappeared: %s" engine-asset-id))
            (database/put-entity! db :assets (assoc asset :engine-asset-id nil)))))
      db (database/get-entities db :assets))))

(defn- dispatch-asset-type-asset-group-sync!
  [db]
  (send asset-type-asset-group-sync-agent 
    (fn [_ db erc-connection]
      (try
        (ekahau.engine.connection/with-engine-connection erc-connection
          (ensure-asset-types-have-engine-asset-groups! db)
          (sync-asset-type-group-assets! db))
        (catch Throwable e
          (error "Error synchronizing asset types to engine asset groups" e))))
    db
    ekahau.engine.connection/*engine-connection*))

(defn load-model-to-database!
  [db]
  (info "Loading model...")
  (let [model-id (get-active-model-id-from-erc)
        db (-> db
             (make-active-model-inactive)
             (insert-active-model model-id)
             (load-map-and-zones (ekahau.engine.cmd/epe-get-model model-id))
             (start-loading-map-images model-id)
             (ensure-asset-engine-assets-exist!)
             (dispatch-asset-type-asset-group-sync!))])
  (info "Model loaded."))

;; Tags and Assets ;;

(defn- parse-position-point
  [properties]
  (let [x (parse-double (:posx properties))
        y (parse-double (:posy properties))]
    [x, y]))

(defn- parse-position
  [properties]
  (if-let [mapid-str (:posmapid properties)]
    (struct-map database/position
      :map-id   (parse-id mapid-str)
      :point    (parse-position-point properties)
      :zone-id  (parse-id (:poszoneid properties))
      :model-id (parse-id (:posmodelid properties)))
    nil))

(defn- parse-timestamp
  [properties]
  (if-let [s (:postime properties)]
    (parse-long s)))

(defn- parse-position-observation
  [properties]
  (struct-map database/position-observation
    :position  (parse-position properties)
    :timestamp (parse-timestamp properties)))

(defn- get-latest-position
  [params]
  (when-first [emsg (ekahau.engine.cmd/epe-call "pos/taglist" (merge params {:fields "posgood"}))]
    (parse-position-observation (:properties emsg))))

(defn get-latest-position-observation-by-tag-id
  [tag-id]
  (get-latest-position {:tagid tag-id}))

(defn- get-latest-position-observation-by-asset-id
  [asset-id]
  (get-latest-position {:assetid asset-id}))

(defn get-tag-asset-id
  "Returns the id of an asset bound to the given tag."
  [tag-id]
  (when-first [result (ekahau.engine.cmd/epe-call "pos/taglist" {:tagid tag-id})]
    (parse-id (-> result :properties :assetid))))

(defn get-asset-tag-id
  "Returns the id of a tag bound to the given asset."
  [asset-id]
  (when-first [result (ekahau.engine.cmd/epe-call "pos/taglist" {:assetid asset-id})]
    (parse-id (-> result :properties :tagid))))

(defn- select-keys-by-pred
  [m pred]
  (select-keys m (map key (filter (comp pred val) m))))

(defn get-matching-tags
  "Returns a map that has engine asset id keys and tag key-property-map where
  values have the given text as substring."
  [text]
  (->>
    (ekahau.engine.cmd/epe-call "pos/taglist" {:fields "user;type" :text text})
    (filter #(-> % :properties :assetid))
    (map
      (fn [{{:keys [assetid] :as properties} :properties}]
        [(parse-id assetid) (select-keys (select-keys-by-pred properties (substring-pred? text)) [:mac :serialnumber :name])]))
    (into {})))

;; ... asset creation and binding

(defn- list-all-asset-ids
  []
  (let [result (ekahau.engine.cmd/epe-call "asset/assetlist")]
    (map
      #(-> % :properties :assetid parse-id)
      result)))

(defn create-asset!
  "Creates an asset to EPE and returns the id of the newly created asset."
  [name]
  (let [result (ekahau.engine.cmd/epe-call "asset/assetcreate" {:name name})]
    (parse-id (-> result first :properties :assetid))))

(defn bind-tag-to-asset!
  "Binds a tag to an asset and returns nil."
  [tag-id asset-id]
  (ekahau.engine.cmd/epe-call "asset/tagbind" {:tagid tag-id :assetid asset-id}))

(defn unbind-tag-from-asset!
  "Removes a tag binding from the specified asset."
  [asset-id]
  (ekahau.engine.cmd/epe-call "asset/tagbind" {:assetid asset-id}))

;; ... asset groups

(def asset-type-asset-group-sync-agent (agent nil))

(defn- create-asset-group!
  [name description]
  (let [result (ekahau.engine.cmd/create-asset-group name description)]
    (-> result first :properties :assetgroupid parse-id)))

(defn- new-group-with-assets!
  [name description engine-asset-ids]
  (let [engine-asset-group-id (create-asset-group! name description)]
    (doseq [engine-asset-id engine-asset-ids]
      (ekahau.engine.cmd/add-asset-to-groups engine-asset-id [engine-asset-group-id]))))

(defn- engine-asset-group-exists?!
  [engine-asset-group-id]
  (boolean (seq (ekahau.engine.cmd/list-asset-groups engine-asset-group-id))))

(defn- asset-type-has-engine-asset-group?!
  [asset-type]
  (when-let [engine-asset-group-id (:engine-asset-group-id asset-type)]
    (engine-asset-group-exists?! engine-asset-group-id)))

(defn- create-asset-group-for-asset-type!
  [asset-type]
  (create-asset-group! (str
                         "_"
                         (:name asset-type)
                         " "
                         (java.util.UUID/randomUUID))
    "Automatically created by Vision."))

(defn assoc-new-empty-asset-group-to-asset-type!
  [asset-type]
  (assoc asset-type 
    :engine-asset-group-id (create-asset-group-for-asset-type! asset-type)))

(defn- ensure-asset-types-have-engine-asset-groups!
  [db]
  (info "Ensuring asset-types have engine-asset-groups ...")
  (reduce
    (fn [db asset-type]
      (if-not (asset-type-has-engine-asset-group?! asset-type)
        (database/put-entity! db :asset-types (assoc-new-empty-asset-group-to-asset-type! asset-type))
        db))
    db (database/get-entities db :asset-types))
  (info "Done. All asset-types have engine-asset-groups."))

(defn- get-engine-asset-ids-by-group!
  []
  (let [results (ekahau.engine.cmd/list-assets)]
    (reduce
      (fn [m {{:keys [assetid assetgroupid] :or {assetgroupid ""}} :properties}]
        (let [asset-id (parse-id assetid)
              asset-group-ids (map parse-id
                                (if (string? assetgroupid)
                                  [assetgroupid]
                                  assetgroupid))]
          (reduce
            (fn [m asset-group-id]
              (assoc m asset-group-id (conj (get m asset-group-id #{}) asset-id)))
            m asset-group-ids)))
      {} results)))

(defn- add-group-entries-for-asset
  [m engine-asset-group-id asset-ids]
  (reduce
    (fn [m asset-id]
      (assoc m asset-id (conj (get m asset-id #{}) engine-asset-group-id)))
    m asset-ids))

(defn- get-engine-asset-group-adds-and-removes!
  [db]
  (let [engine-asset-ids-by-group (get-engine-asset-ids-by-group!)]
    (reduce
      (fn [m {:keys [asset-type assets]}]
        (let [engine-asset-group-id (:engine-asset-group-id asset-type)
              engine-asset-ids-of-assets (set (remove nil? (map :engine-asset-id assets)))
              engine-asset-ids-of-engine-asset-group (get engine-asset-ids-by-group engine-asset-group-id)
              to-be-added (clojure.set/difference engine-asset-ids-of-assets
                            engine-asset-ids-of-engine-asset-group)
              to-be-removed (clojure.set/difference engine-asset-ids-of-engine-asset-group
                              engine-asset-ids-of-assets)]
          (-> m
            (update-in [:adds] add-group-entries-for-asset engine-asset-group-id to-be-added)
            (update-in [:removes] add-group-entries-for-asset engine-asset-group-id to-be-removed))))
      {:adds {}
       :removes {}}
      (list-assets-by-asset-type db))))

(defn- sync-asset-type-group-assets!
  [db]
  (info "Syncing asset-types & engine-asset-groups...")
  (let [{:keys [adds removes]} (get-engine-asset-group-adds-and-removes! db)]
    (doseq [[engine-asset-id asset-group-ids] removes]
      (info (format "Removing engine-asset %s from groups %s" engine-asset-id asset-group-ids))
      (ekahau.engine.cmd/remove-asset-from-groups engine-asset-id asset-group-ids))
    (doseq [[engine-asset-id asset-group-ids] adds]
      (info (format "Adding engine-asset %s to groups %s" engine-asset-id asset-group-ids))
      (ekahau.engine.cmd/add-asset-to-groups engine-asset-id asset-group-ids)))
  (info "Done. Synchronized asset-types & engine-asset-groups."))

(defn- synchronize-asset-type-groups!
  [db]
  (let [db (ensure-asset-types-have-engine-asset-groups! db)]
    (sync-asset-type-group-assets! db)
    db))

;; Users ;;

(defn list-users!
  []
  (map
    (fn [{{:keys [rolename name userid useruid]} :properties}]
      {:name name
       :role rolename
       :uid useruid})
    (ekahau.engine.cmd/list-users!)))

;; Misc ;;

(defn- get-epe-status!
  []
  (-> (ekahau.engine.cmd/get-status!)
    (first)
    (:properties)
    (select-keys [:buildid :version :uptime :starttime :servertime])))

(defn get-epe-start-date!
  []
  (.getTime (.parse (SimpleDateFormat. "yyyy-MM-dd hh:mm:ssZ" Locale/US)
              (:starttime (get-epe-status!)))))

(defn- epe-get-current-time!
  []
  (-> (get-epe-status!)
    :servertime
    (parse-long)))

(defn get-ERC-route-history-info!
  []
  (-> (ekahau.engine.cmd/routehistoryinfo)
    (first)
    (:properties)
    (update-in [:count] parse-long)
    (update-in [:firsttimestamp] parse-long)
    (update-in [:lasttimestamp] parse-long)))

(defn- get-epe-version-string!
  []
  (-> (get-epe-status!)
    :version))

(defn- get-version-components
  [s]
  (vec (map parse-int (rest (re-find #"(\d+)\.(\d+)\.(\d+)-.*" s)))))

(defn- get-epe-version-components!
  []
  (get-version-components (get-epe-version-string!)))

(defn- engine-user-to-vision-user
  [user]
  {:uid (:useruid user)
   :name (:name user)
   :role (:rolename user)})

(defn get-user!
  []
  (-> (ekahau.engine.cmd/get-user!)
    (first)
    :properties
    (engine-user-to-vision-user)))

(defn- connected-to-erc?!
  ([{:keys [host port] :as custom-properties}]
    (try
      (let [erc-conn (ekahau.engine.connection/create-unauthenticated-engine-connection
                       custom-properties)]
        (ekahau.engine.connection/with-engine-connection erc-conn
          (get-epe-status!)
          true))
      (catch EException e
        (if (= 401 (.getErrorInt e)) ;Unauthorized
          true
          (do (error "Cant connect to ERC" e) false)))
      (catch Exception e
        (do (error "Cant connect to ERC" e) false))))
  ([]
    (connected-to-erc?! (load-erc-config!))))

(defn- authenticated-to-erc?!
  ([{:keys [host port user password] :as custom-properties}]
    (try 
      (let [erc-conn (ekahau.engine.connection/create-engine-connection 
                       custom-properties)] 
        (ekahau.engine.connection/with-engine-connection erc-conn
          (get-epe-status!)
          [true erc-conn]))
      (catch Exception e
        (do (error "Cant authenticate to ERC" e) [false nil]))))
  ([]
    (authenticated-to-erc?! (load-erc-config!))))

(defn- license-expired?
  [license]
  (if-let [m (re-matches #"in (.+?) days?" (:Expires license))]
    (<= (parse-long (second m)) 0)
    (<= (.getTime (.parse (SimpleDateFormat. "dd.MM.yyyy" Locale/US) (:Expires license)))
      (System/currentTimeMillis))))

(defn- get-erc-license!
  []
  (-> (ekahau.engine.cmd/get-license!) (first) :properties
    (select-keys [:EndUser :Positioning :Vision :TagLimit :Expires :Created :LicenseFileCount])
    (update-in [:LicenseFileCount] parse-long)
    (update-in [:Vision]      parse-boolean)
    (update-in [:Positioning] parse-boolean)
    (#(assoc % :Expired (cond
                          (nil? (:Expires %))      nil
                          (= "Never" (:Expires %)) false
                          :else                    (license-expired? %))))))

(defn- get-erc-active-model!
  []
  (-> (ekahau.engine.cmd/epe-get-active-model) (first) :properties
    (select-keys [:modifiedon :description :name :version :modelid])))

(defn- get-erc-diagnosis!
  []
  (-> (ekahau.engine.cmd/get-diagnosis!) (first) :properties))

(defonce *vs-connected-key*     :connected-to-ERC)
(defonce *vs-authenticated-key* :authenticated-to-ERC)
(defonce *vs-license-key*       :ERC-license)
(defonce *vs-model-key*         :ERC-active-model)
(defonce *vs-system-key*        :ERC-system-info)

(def *all-vs-keys* [*vs-connected-key* 
                    *vs-authenticated-key*
                    *vs-license-key*
                    *vs-model-key*
                    *vs-system-key*])

(defn get-erc-status!
  [erc-config]
  (let [initial-status (into {} (map #(vector % nil) *all-vs-keys*))]
    (if-let [connected-to-erc? (connected-to-erc?! erc-config)]
      (let [[authenticated-to-erc? authenticated-connection] (authenticated-to-erc?! erc-config)]
        (if authenticated-to-erc?
          (let [[erc-license erc-active-model erc-status]
                (ekahau.engine.connection/with-engine-connection authenticated-connection
                  [(get-erc-license!) (get-erc-active-model!) (get-epe-status!)])]
            (merge initial-status
              {*vs-connected-key*     true
               *vs-authenticated-key* true
               *vs-license-key*       erc-license
               *vs-model-key*         erc-active-model
               *vs-system-key*        erc-status}))
          (merge initial-status
            {*vs-connected-key*     true
             *vs-authenticated-key* false})))
      (merge initial-status
        {*vs-connected-key* false}))))

;; Tag messages ;;

(defn- send-tag-command!
  [tag-ids command]
  (doall
    (map
      (fn [tag-id]
        (ekahau.engine.cmd/add-tag-command! tag-id command))
      tag-ids)))

(defn- get-tags-supporting-messages
  [tag-ids]
  (->> (ekahau.engine.cmd/epe-get-tags {:capability "messaging" :tagid tag-ids})
    (map (comp parse-id :tagid :properties))))

(defn send-instant-message!
  [tag-ids message]
  (let [tag-ids (get-tags-supporting-messages tag-ids)]
    (send-tag-command! tag-ids (str "mdi " message))))

(defn send-text-message!
  [tag-ids message]
  (let [tag-ids (get-tags-supporting-messages tag-ids)]
    (send-tag-command! tag-ids (str "mdt " message))))

;; Event Rules ;;

(defn create-event-rule!
  [event-rule-params]
  (-> (ekahau.engine.cmd/create-event-rule event-rule-params)
    first :properties :ruleid parse-id))

(defn delete-event-rule!
  [id]
  (ekahau.engine.cmd/delete-event-rule! id))

(defn- rule-status->kw
  [status]
  (condp = status
    "ACTIVE" :active
    "DISABLED" :disabled
    "DELETED" :deleted))

(defn list-engine-event-rule-statuses!
  []
  (map
    (fn [{{:keys [status ruleid]} :properties}]
      {:id (parse-id ruleid)
       :status (rule-status->kw status)})
    (ekahau.engine.cmd/list-event-rules!)))

(defn event-rule-active?
  [engine-rule-id]
  (let [{{status :status} :properties} (first (ekahau.engine.cmd/get-event-rule! engine-rule-id))]
    (= status "ACTIVE")))

(defn enable-event-rule!
  [engine-rule-id]
  (try
    (ekahau.engine.cmd/enable-event-rule! engine-rule-id)
    (catch com.ekahau.common.sdk.EException e)))

(defn disable-event-rule!
  [engine-rule-id]
  (try
    (ekahau.engine.cmd/disable-event-rule! engine-rule-id)
    (catch com.ekahau.common.sdk.EException e)))
