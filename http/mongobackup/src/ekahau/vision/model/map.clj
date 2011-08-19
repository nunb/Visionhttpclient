(ns ekahau.vision.model.map
  (:require
    [ekahau.vision.database :as database])
  (:use
    [clojure.set :only (select join rename)]
    [ekahau.math :only (polygon-includes?)]))

(defn area-includes?
  [area position]
  (and
    (= (:map-id area) (:map-id position))
    (polygon-includes? (:polygon area) (:point position))))

(defn- join-assets-to-position-observations
  [assets asset-position-observations]
  (join
    (select :engine-asset-id (set assets))
    (rename (set (map #(into {} %) asset-position-observations)) {:id :engine-asset-id})))

(defn- is-on-map?
  [asset map-id]
  (= (-> asset :position-observation :position :map-id) map-id))

(defn get-assets-with-position-observations
  [db map-id]
  (select
    #(is-on-map? % map-id)
    (join-assets-to-position-observations
      (database/get-entities db :assets)
      (database/get-entities db :asset-position-observations))))
