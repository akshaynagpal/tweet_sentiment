<?php
require 'vendor/autoload.php';


if ('POST' !== $_SERVER['REQUEST_METHOD']) {
    http_response_code(405);
    echo "POST REQUEST NOT DETECTED!";
    die;
}

// use vendor\aws\aws-php-sns-message-validator\src\Message;
// use vendor\aws\aws-php-sns-message-validator\src\MessageValidator;
use Aws\Sns\Message;
use Aws\Sns\MessageValidator;

// try {
    $message = Message::fromRawPostData();
    $validator = new MessageValidator();
    $validator->validate($message);

    if(in_array($message['Type'], ['SubscriptionConfirmation', 'UnsubscribeConfirmation'])) 
    {
        file_get_contents($message['SubscribeURL']);
        $msg = json_decode($message);
        echo $msg;
    }
    if ($message['Type'] === 'Notification') {
   		
        $msg2 = $message['Message'];
        $recieved_message = json_decode($msg2,true);
        $json_data = array(
            "id" => $recieved_message["id"],
            "lat" => $recieved_message["lat"],
            "long" => $recieved_message["long"],
            "sentiment" => $recieved_message["sentiment"]
            );
        // $myfile = fopen("myfile.txt", "a") or die("unable to open file!");
        // fwrite($myfile, $recieved_message["id"].$recieved_message["lat"].$recieved_message["long"]);
        // fclose($myfile);
        $json_data = json_encode($json_data);
        $endpoint = "http://search-twitttrends-zh3numc3nqottfkwdki7lvys54.us-east-1.es.amazonaws.com/".$recieved_message["topic"]."/tweet";
        $ci = curl_init();
        curl_setopt($ci, CURLOPT_URL, $endpoint);
        //curl_setopt($ci, CURLOPT_PORT, $search_port);
        curl_setopt($ci, CURLOPT_TIMEOUT, 200);
        curl_setopt($ci, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ci, CURLOPT_FORBID_REUSE, 0);
        curl_setopt($ci, CURLOPT_CUSTOMREQUEST, 'POST');
        curl_setopt($ci, CURLOPT_POSTFIELDS, $json_data);
        $response = curl_exec($ci);
	}
// } catch (Exception $e) {
//     http_response_code(404);
//     die;
// }

?>