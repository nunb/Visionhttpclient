(ns ekahau.vision.par-snapshot)

;; Common utility ns for par-management, par-history-service
;; Holds the data structure & operations common for both nses.

;;
;; Par snapshot data structure:
;; ------------------
;; {:a-z-m  ---> asset-zone map
;;      {asset-id1 => zone-id1
;;       asset-id2 => zone-id2
;;       ...}
;;
;;  :z-at-m  ---> zone-asset-type-counts map
;;      {:zone-id1
;;          {:asset-type-id1 => count1
;;           :asset-type-id2 => count2
;;           ...}
;;       :zone-id2
;;          {:asset-type-id1 => count1
;;           :asset-type-id2 => count2
;;           ...}
;;       ...}}
;;
;; Functioning / operations:
;; ------------------------
;; When a new position observation comes it has :assetid, :zoneid
;; it doesn't have :previous-zone-id.
;;
;; 1) Updating :a-z-m
;;    We lookup the previous-zone-id in :a-z-m
;;    and update the new zoneid for the assetid.
;;
;; 2) Updating :z-at-m
;;    For the previous-zone-id, we decrease the asset-type count
;;    For the new-zone-id, we increase the asset-type count
;;
;; As an optimization we do nothing if new-zone-id = previous-zone-id
;;


(defn update-par-snapshot
  [par-snapshot {:keys [assetid zoneid assettypeid]}]
  (let [oldzid (get-in par-snapshot [:a-z-m assetid])]
    (if (= oldzid zoneid)
      par-snapshot
      (-> par-snapshot
        (assoc-in [:a-z-m assetid] zoneid)
        (update-in [:z-at-m oldzid assettypeid] #(if % (dec %) nil))
        (update-in [:z-at-m zoneid assettypeid] #(if % (inc %) 1))))))

(defn get-curr-zoneid
  [par-snapshot assetid]
  (get-in par-snapshot [:a-z-m assetid]))

(defn get-assets-count
  [par-snapshot zoneid assettypeid]
  (or (get-in par-snapshot [:z-at-m zoneid assettypeid]) 0))
