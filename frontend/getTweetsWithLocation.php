<?php
  // method which pulls tweets which have location and sentiment from ElasticSearch
	function getTweetsWithLocation($keyword){
		$ci = curl_init();
		curl_setopt($ci, CURLOPT_URL, "https://search-twitttrends-zh3numc3nqottfkwdki7lvys54.us-east-1.es.amazonaws.com/".$keyword."/_search?size=10000");
  		curl_setopt($ci, CURLOPT_HTTPHEADER, array('Accept: application/json'));
  		curl_setopt($ci, CURLOPT_RETURNTRANSFER, true);
  		$get_output = curl_exec($ci);
  		$json_output = json_decode($get_output,true);
  		curl_close($ci);
  		$tweets = $json_output['hits']['hits'];
  		$num_tweets =  sizeof($tweets);
  		$geoArray = array();
  		for ($i=0; $i < $num_tweets; $i++) {
    		$geoArray[$i]["lat"] = $tweets[$i]['_source']['lat']; 
    		$geoArray[$i]["long"] = $tweets[$i]['_source']['long'];

        //coloring marker based on sentiment
        $sent = $tweets[$i]['_source']['sentiment'];
        if($sent=="positive"){
          $geoArray[$i]["sentiment"] = "00FF00"; //Green
        }
        elseif ($sent=="negative") {
          $geoArray[$i]["sentiment"] = "FF0000";  // Red
        }
        else{
          $geoArray[$i]["sentiment"] = "0000FF";  // Blue
        }
  		}
  		return $geoArray;
	}
?>