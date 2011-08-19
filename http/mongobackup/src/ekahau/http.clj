(ns ekahau.http
  (:import
    [org.apache.commons.httpclient HostConfiguration HttpClient HttpMethod Header]
    [org.apache.commons.httpclient.methods GetMethod PostMethod]))

(defn- get-response-headers
  [^HttpMethod method]
  (map #(.toString ^Header %) (.getResponseHeaders method)))

(defn execute-request
  [^HttpMethod method]
  (let [client (HttpClient.)
        status (.executeMethod client method)]
    {:status status 
     :body (String. ^bytes (.getResponseBody method) "UTF-8")
     :headers (get-response-headers method)}))

(defn execute-get-request 
  [url]
  (execute-request (GetMethod. url)))

(defn execute-post-request 
  [url]
  (execute-request (PostMethod. url)))
