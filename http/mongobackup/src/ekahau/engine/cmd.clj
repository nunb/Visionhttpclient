(ns ekahau.engine.cmd
  (:use
    [ekahau.engine.connection]
    [ekahau.engine.emsg]))

;; Generic EPE Commands ;;

(defn epe-call
  [path & params]
  (unpack-eresponse (apply epe-call-raw path params)))

;; Specific EPE Commands ;;

(defn epe-echo
  [text]
  (epe-call (str "test/echo?" text)))

(defn epe-get-model
  [modelid]
  (epe-call "mod/modelbrowse" {:modelid modelid}))

(defn epe-get-active-model
  []
  (epe-call "mod/modelgetactive"))

(defn epe-get-tags
  ([]
    (epe-get-tags {:fields "all"}))
  ([params]
    (epe-call "pos/taglist" params)))

(defn epe-get-rules
  []
  (epe-call "eve/rulelist"))

(defn epe-get-rule
  [id]
  (epe-call "eve/rulebrowse" {:ruleid id}))

(defn epe-get-events
  []
  (epe-call "eve/historylist"))

(defn epe-get-events-within
  [x]
  (epe-call "eve/historylist" {:within x}))

(defn epe-get-events-num-latest
  [x]
  (epe-call "eve/historylist" {:numlatest x}))

(defn epe-get-events-num-latest-until
  [num time]
  (epe-call "eve/historylist" {:numlatest num :until time}))

(defn epe-get-events-until-event
  [num id]
  (epe-call "eve/historylist" {:numlatest num :untilid id}))

(defn epe-get-events-of-rule
  [id]
  (epe-call "eve/historylist" {:ruleid id}))

(defn epe-delete-rule
  [id]
  (epe-call "eve/ruledelete" {:ruleid id}))

(defn epe-purge-rule
  [id]
  (epe-call "eve/rulepurge" {:ruleid id}))

(defn routehistoryinfo
  []
  (epe-call "route/routehistoryinfo"))

(defn routehistorybrowse
  [opts]
  (epe-call "route/routehistorybrowse" opts))

(defn routehistorybrowse-by-asset
  [asset-id]
  (routehistorybrowse {:assetid asset-id :numlatest 10}))

(defn routehistorybrowse-by-asset-since
  [asset-id timepoint route-point-count]
  (routehistorybrowse {:assetid asset-id :numlatest route-point-count :since timepoint}))

(defn routehistorybrowse-by-asset-until
  [asset-id timepoint route-point-count]
  (routehistorybrowse {:assetid asset-id :numoldest route-point-count :until timepoint}))

(defn routehistorybrowse-by-asset-within
  [asset-id [since until]]
  (routehistorybrowse {:assetid asset-id :since since :until until}))

(defn routehistorybrowse-by-asset-latest-points
  [asset-id route-point-count]
  (routehistorybrowse {:assetid asset-id :numlatest route-point-count}))

(defn routehistorybrowse-by-tags
  [tag-ids start-time end-time]
  (routehistorybrowse {:tagid tag-ids :since start-time :until end-time}))

(defn routehistorysnapshot-by-tags
  [tag-ids time]
  (routehistorybrowse {:tagid tag-ids :time time}))

(defn routehistorysnapshot-by-assets
  [engine-asset-ids time]
  (when (and engine-asset-ids
          (or (not (coll? engine-asset-ids))
            (seq engine-asset-ids)))
    (epe-call "route/routehistorysnapshot" {:assetid engine-asset-ids :time time})))

(defn create-event-rule
  [parameters]
  (epe-call "eve/rulecreate" parameters))

(defn create-asset-group
  [name description]
  (epe-call "asset/assetgroupcreate" {:name name :description description}))

(defn delete-asset-group
  [id]
  (epe-call "asset/assetgroupdelete" {:assetgroupid id}))

(defn edit-asset-group
  [id name description]
  (epe-call "asset/assetgroupedit" {:assetgroupid id :name name :description description}))

(defn list-asset-groups
  ([]
    (epe-call "asset/assetgrouplist"))
  ([asset-group-id]
    (epe-call "asset/assetgrouplist" {:assetgroupid asset-group-id})))

(defn add-asset-to-groups
  [id group-ids]
  (epe-call "asset/assetaddtogroups" {:assetid id :assetgroupid group-ids}))

(defn remove-asset-from-groups
  [id group-ids]
  (epe-call "asset/assetremovefromgroups" {:assetid id :assetgroupid group-ids}))

(defn list-assets
  []
  (epe-call "asset/assetlist"))

(defn add-tag-command!
  [tag-id command ]
  (epe-call "cfg/tagcommandadd" {:cmd command :tagid tag-id}))

(defn list-users!
  []
  (epe-call "sys/userlist"))

(defn get-user!
  []
  (epe-call "sys/userbrowse"))

(defn list-event-rules!
  []
  (epe-call "eve/rulelist"))

(defn delete-event-rule!
  [rule-id]
  (epe-call "eve/ruledelete" {:ruleid rule-id}))

(defn get-event-rule!
  [rule-id]
  (epe-call "eve/rulebrowse" {:ruleid rule-id}))

(defn enable-event-rule!
  "Enables event rule on engine.
  NOTE: Throws exception if rule already enabled"
  [rule-id]
  (epe-call "eve/ruleenable" {:ruleid rule-id}))

(defn disable-event-rule!
  "Disables event rule on engine.
  NOTE: Throws exception if rule already disabled"
  [rule-id]
  (epe-call "eve/ruledisable" {:ruleid rule-id}))

(defn get-status!
  []
  (epe-call "sys/_status"))

(defn get-license!
  []
  (epe-call "sys/licenseget"))

(defn get-diagnosis!
  []
  (epe-call "sys/diagnose"))
