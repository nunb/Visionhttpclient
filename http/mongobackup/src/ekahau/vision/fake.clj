(ns ekahau.vision.fake
  (:require
   ekahau.vision.par-management
   [ekahau.UID :as uid]
   [ekahau.vision.database :as database]
   [ekahau.vision.engine-service]
   [ekahau.vision.model.positions :as positions]
   [ekahau.vision.server]))

(defn create-fake-floor-image
  [name]
  (let [width 1200
        height 800
        image (java.awt.image.BufferedImage.
               width height java.awt.image.BufferedImage/TYPE_INT_RGB)]
    (let [g (.createGraphics image)]
      (try
        (.setRenderingHint g
                           java.awt.RenderingHints/KEY_RENDERING
                           java.awt.RenderingHints/VALUE_RENDER_QUALITY)
        (.setRenderingHint g
                           java.awt.RenderingHints/KEY_ANTIALIASING
                           java.awt.RenderingHints/VALUE_ANTIALIAS_ON)
        (.setColor g java.awt.Color/WHITE)
        (.fillRect g 0 0 width height)
        (.setColor g java.awt.Color/LIGHT_GRAY)
        (let [grid-edge 100]
          (doseq [x (iterate (partial + grid-edge) 0) :while (< x width)
                  y (iterate (partial + grid-edge) 0) :while (< y height)]
            (.fillRect g
                       (+ 2 x) (+ 2 y)
                       (- grid-edge 4) (- grid-edge 4))))
        (.setColor g java.awt.Color/BLACK)
        (.setFont g (java.awt.Font. "Arial" java.awt.Font/BOLD 18))
        (.drawString g name 30 50)
        (finally
         (.dispose g))))
    image))

(defn save-image-as-png
  [image file]
  (javax.imageio.ImageIO/write image "png" file))

(defn png-bytes-from-image
  [image]
  (let [bout (java.io.ByteArrayOutputStream.)]
    (with-open [out bout]
      (javax.imageio.ImageIO/write image "png" out))
    (.toByteArray bout)))

(defn create-delayed-image
  [map-id name]
  (delay
   (ekahau.vision.engine-service/cache-image-data!
    map-id
    (png-bytes-from-image
     (create-fake-floor-image name)))))

(defn add-ten-fake-floors-to-building
  [db building-id building-name]
  (doseq [n (map inc (range 10))]
    (let [map-id (uid/gen)
          name (str building-name " - Floor " n)]
      (database/put-entity!
       db :maps
       (struct-map database/model-map
         :id map-id
         :name name
         :scale 20.0
         :image (create-delayed-image map-id name)))
      (database/put-entity!
       db :floors
       (struct-map database/floor
         :id (uid/gen)
         :map-id map-id
         :building-id building-id
         :order-num n)))))

(defn add-ten-fake-buildings
  [db base-name]
  (doseq [n (map inc (range 10))]
    (let [building-id (uid/gen)
          building-name (str base-name " " n)]
     (database/put-entity!
      db :buildings
      (struct-map database/building
        :id building-id
        :name building-name))
     (add-ten-fake-floors-to-building db building-id building-name)
     )))

(defn create-fake-location
  [erc-asset-id map-id zone-id model-id [x y :as point] timestamp]
  (struct-map database/entity-position-observation
    :id erc-asset-id
    :position-observation (struct-map database/position-observation
                            :position (struct-map database/position
                                        :map-id   map-id
                                        :zone-id  zone-id
                                        :model-id model-id
                                        :point    point)
                            :timestamp timestamp)))

(defn post-fake-position-observation
  [position-observation]
  (let [queue (-> @ekahau.vision.server/*server-agent*
                  :engine
                  :location-manager
                  :loc-tracker
                  :loc-queue)]
    (.put queue position-observation)))

(defn bounding-box-of-points
  [points]
  (reduce
   (fn [result [x y]]
     (-> result
         (update-in [:min-x] (fnil min x) x)
         (update-in [:max-x] (fnil max x) x)
         (update-in [:min-y] (fnil min y) y)
         (update-in [:max-y] (fnil max y) y)))
   {} points))

(defn center-of-bounding-box
  [{:keys [min-x max-x min-y max-y]}]
  [(double (+ min-x (/ (- max-x min-x) 2)))
   (double (+ min-y (/ (- max-y min-y) 2)))])

(defn update-position-to-zone
  [db asset-id zone-id]
  (let [asset (database/get-entity-by-id db :assets asset-id)
        {{:keys [polygon map-id]} :area}
        (database/get-entity-by-id db :zones zone-id)
        model (first (filter :active (database/get-entities db :models)))]
    (post-fake-position-observation
     (create-fake-location
      (:engine-asset-id asset)
      map-id
      zone-id
      (:id model)
      (center-of-bounding-box
       (bounding-box-of-points polygon))
      (- (System/currentTimeMillis) 5000)))))

(defn move-assets-to-zone
  [db asset-ids zone-id]
  (doseq [asset-id asset-ids]
    (update-position-to-zone db asset-id zone-id)))

(defn position-observation-zone-id
  [zone-id]
  [[ "=" [:position-observation :position :zone-id] zone-id ]])

(defn assets-on-zone
  [db zone-id]
  (database/search-entities db
                            :asset-position-observations
                            (position-observation-zone-id zone-id)))

(defn move-all-assets-from-zone-to-zone
  [db from-zone-id to-zone-id]
  (move-assets-to-zone
   db
   (->> (assets-on-zone db from-zone-id)
        (map :id))
   to-zone-id))

(defn select-random-assets
  [db n]
  (take n (shuffle (database/get-entities db :assets))))

(defn active-par-event-rule-names-and-zone-ids
  [db]
  (map (juxt :name (comp :zone-id :trigger))
       (ekahau.vision.par-management/get-active-par-management-rules db)))