(ns ekahau.vision.tools
  (:use
    [clojure.contrib.logging :only [info warn]])
  (:require
    ekahau.vision.asset-import
    ekahau.vision.routes.asset))

(defn bind-by-csv
  [db engine csv-file id-property-key]
  (let [binding-list (ekahau.vision.asset-import/create-asset-tag-binding-list
                       db
                       csv-file
                       id-property-key)]
    (doseq [[asset-id tag-id] binding-list]
      (info (format "Binding [%s %s]" asset-id tag-id))
      (try
        (ekahau.vision.routes.asset/bind-tag-to-asset! db engine tag-id asset-id)
        (catch com.ekahau.common.sdk.EException e
          (warn (format "Failed to bind [%s %s]" asset-id tag-id)))))))

(comment
  (bind-by-csv db
    engine
    "private/samc_asset_information_export__hospital_id_to_tag_id.csv"
    [2 1 0])
  (bind-by-csv db
    engine
    "private/samc_people__name_to_tag_id.csv"
    [3 0 0]))
