(ns ekahau.vision.model.zone-grouping
  (:require
   [ekahau.vision.database :as database])
  (:use
   [clojure.contrib.trace :only [trace]]))

(defn get-zone-groupings
  [db]
  (database/get-entities db :zone-groupings))

(defn get-zone-grouping-by-id
  [db zone-grouping-id]
  (database/get-entity-by-id db :zone-groupings zone-grouping-id))

(defn get-zone-group-by-id
  [db zone-grouping-id zone-group-id]
  (when-let [zone-grouping (get-zone-grouping-by-id db zone-grouping-id)]
    (first (filter #(= zone-group-id (str (:id %)))
                   (:groups zone-grouping)))))

(defn get-zone-grouping-info-for-zone-id
  [db zone-id]
  (vec
    (remove #(nil? (:zone-group %))
      (map (fn [z]
             {:zone-grouping (select-keys z [:id :name])
              :zone-group (when-let
                            [zg (first
                                  (filter #(contains?
                                             (set (:zone-ids %))
                                             zone-id)
                                    (:groups z)))]
                            (select-keys zg [:id :name]))})
        (get-zone-groupings db)))))

(defn- zone-group-of-zone-entries
  [db]
  (for [zone-grouping (get-zone-groupings db)
        :let [zone-grouping-id
              (:id zone-grouping)
              groups (:groups zone-grouping)]
        zone-group groups
        zone-id (:zone-ids zone-group)]
    [zone-id {:zone-grouping zone-grouping :zone-group zone-group}]))

(defn create-zone-group-entries-by-zone-id-map
  "Returns a map that has zone-id keys and zone group entry values.
  Each entry has :zone-grouping and :zone-group keys."
  [db]
  (reduce
   (fn [result [zone-id zone-group-entry]]
     (update-in result [zone-id] (fnil conj []) zone-group-entry))
   {} (zone-group-of-zone-entries db)))

(defn get-zone-ids-of-zone-group
  [db zone-grouping-id zone-group-id]
  (when-let [zone-group (get-zone-group-by-id db
                                              zone-grouping-id
                                              zone-group-id)]
    (:zone-ids zone-group)))
