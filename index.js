const formData = require('form-data');
const { Storage } = require('@google-cloud/storage');
const Mailgun = require('mailgun.js');
const mailgun = new Mailgun(formData);
const axios = require('axios');
const AWS = require('aws-sdk');


const dynamodb = new AWS.DynamoDB.DocumentClient();

const decodedString = Buffer.from(process.env.GCP_SECRET_KEY, 'base64').toString('utf-8');
const gcpCredentials = JSON.parse(decodedString);

const storage = new Storage({ 
    projectId: gcpCredentials.projectId,
    credentials: gcpCredentials 
});
const bucket = storage.bucket(process.env.BUCKET_NAME);

const mailgunObj = mailgun.client({
    username: 'csye6225',
    key: process.env.MAILGUN_API_KEY 
});


const UploadToGCP = async (snsMessage) => {
    try {
        const gcpBucketName = process.env.BUCKET_NAME
        // Download file
        const response = await axios({
          method: 'GET',
          url: snsMessage.submission_url,
          responseType: 'stream'
        });
    
//const objectName = `Assignment_${snsMessage.email}_${Date.now()}.zip`;
        // Upload to GCP Bucket
        const blob = bucket.file(`Assignment_${snsMessage.email}_${Date.now()}.zip`);
        
        const blobStream = blob.createWriteStream();
    
        return new Promise((resolve, reject) => {
          response.data.pipe(blobStream)
            .on('error', err => reject(err))
            .on('finish', () => resolve(`File uploaded successfully to ${gcpBucketName}`));
        });
    
      } catch (error) {
        console.error(error);
        throw new Error(`Error in processing: ${error.message}`);
      }
};

const sendMail = async (dataMsg) => {

    try {
        const body = await mailgunObj.messages.create('meghnaallam.me', dataMsg); 
        console.log(body);
    } catch (error) {
        console.error(error);
    }
};

exports.handler= async(event)=> {
    console.log(event);
    console.log("received sns event:", JSON.stringify(event,null,2));
    console.log('event record:',event.Records)
    const record = event.Records[0];
    console.log(record.Sns.Message);
    const snsMessage = JSON.parse(record.Sns.Message);
    console.log(snsMessage);
    const receiver_email=snsMessage.email;
    let sender_email = 'rmeghana04@gmail.com';
    let email_subject = 'Regarding your recent assignment submission';
    // let email_body = 'Data received from SNS';

    const dataMsg = {
        from: sender_email,
        to: receiver_email,
        subject: "Regarding your recent assignment submission",
        'h:X-Mailgun-Variables': JSON.stringify({firstname: snsMessage.firstname, lastname: snsMessage.lastname, 
            submission_url:snsMessage.submission_url,gcpBucketName:`Assignment_${snsMessage.email}_${Date.now()}.zip`,
        assignment_name: snsMessage.assignment_name})
    };


    if (snsMessage.submission_url) {
        await UploadToGCP(snsMessage)
            .then(() => {
                dataMsg.template = "successful"
                insertEmailRecordToDynamoDB({
                    id: generateUniqueId(),
                    receiverEmail: receiver_email,
                    emailStatus: 'Delivered',
                    assignmentStatus: 'Submitted'
                });
                
            })
            .catch(error => {
                  dataMsg.template = 'upload failed'
                  insertEmailRecordToDynamoDB({
                    id: generateUniqueId(),
                    receiverEmail: receiver_email,
                    emailStatus: 'Upload failed',
                    assignmentStatus: 'Upload Failed'
                });
            });
        await sendMail(dataMsg);
    }

     function insertEmailRecordToDynamoDB(record) {
        const params = {
            TableName: process.env.TABLE_NAME, 
            Item: record
        };
        return dynamodb.put(params).promise();
    }
    function generateUniqueId() {
        return Date.now().toString() + Math.random().toString(36).substring(2);
    }
}

