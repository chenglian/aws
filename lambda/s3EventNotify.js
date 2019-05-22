// dependencies
var AWS = require('aws-sdk');
var util = require('util');

// get reference to S3 client 
var s3 = new AWS.S3();
var sns = new AWS.SNS(); 

exports.handler =  (event, context, callback) => {

    const eventText = JSON.stringify(event, null, 2);

    // Read options from the event.
    console.log("Reading options from event:\n", util.inspect(event, {depth: 5}));
    const srcBucket = event.Records[0].s3.bucket.name;
    // Object key may have spaces or unicode non-ASCII characters.
    const srcKey    =
    decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));  

    // Sanity check: validate the expected source bucket.
    if (srcBucket != "datafile-upload") {
        console.log("Source bucket does not match!");
        return;
    }
    
    const dstBucket = "datafile-ready";
    const dstKey    = srcKey;

    // Infer the file type.
    var typeMatch = srcKey.match(/\.([^.]*)$/);
    if (!typeMatch) {
        console.log("Could not determine the file type.");
        return;
    }
    var fileType = typeMatch[1];
    if (fileType != "csv") {
        console.log('Unsupported file type: ${fileType}');
        return;
    }

    const params = {
        Bucket: dstBucket, 
        CopySource: srcBucket + '/' + srcKey,
        Key:  dstKey
    };

    s3.copyObject(params, function(copyErr, copyData){
        if (copyErr) {
          console.log(copyErr, copyErr.stack);
          return;
        }
        else {
          console.log("Copied: " + params);
          
            //notify SNS
            const msg = {
                Message: eventText, 
                Subject: srcKey + " has been uploaded to " + srcBucket,
                TopicArn: "arn:aws:sns:eu-west-1:123456789012:datafile-notify"
            };
            sns.publish(msg, function (err, data){
                if (err){
                    console.log(err, err.stack);
                    return;
                }
                else{
                    console.log("Sent message to arn:aws:sns:eu-west-1:123456789012:datafile-notify: ", data);
                    return;
                }
            });
        }
    });

};
