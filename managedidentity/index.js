const axios = require('axios');
const { v4: uuidv4 } = require('uuid');

/********* Axios interceptors to calculate request time *********/
axios.interceptors.request.use((config) => {
    config.metadata = { startTime: new Date() };
    return config;
}, (error) => Promise.reject(error));

axios.interceptors.response.use((response) => {
    response.config.metadata.endTime = new Date();
    response.duration = response.config.metadata.endTime - response.config.metadata.startTime;
    return response;
}, (error) => {
    if (error.config && error.config.metadata) {
        error.config.metadata.endTime = new Date();
        error.duration = error.config.metadata.endTime - error.config.metadata.startTime;
    }
    return Promise.reject(error);
});

/********* Azure Function *********/
module.exports = async function (context, eventHubMessages) {
    try {
        context.log("Processing Event Hub messages...");

        // Connection details
        const api_endpoint = "<O2_ENDPOINT>/api/";
        const org = "default"; // Replace with your organization name
        const auth = {
            username: process.env.AUTH_USERNAME,
            password: process.env.AUTH_PASSWORD
        };        
        const org_url = `${api_endpoint}${org}`;
        const log_stream_prefix = 'microsoft_org_managedidentity_signins_logs';
        const metric_stream_name = 'microsoft_org_managedidentity_signins_metrics';
        const metric_url = `${org_url}/${metric_stream_name}/_json`;

        // Process each Event Hub message
        await Promise.all(eventHubMessages.map(async (message, index) => {
            context.log(`Processing message ${index + 1}: ${message}`);

            let records;
            try {
                records = JSON.parse(message).records;
                context.log(`Parsed records: ${JSON.stringify(records)}`);
            } catch (error) {
                context.log(`Error parsing message: ${message}`, error);
                return; // Skip this message
            }

            if (!records || records.length === 0) {
                context.log("No records to process in this message.");
                return;
            }

            let log_records = {};
            log_records['default'] = [];
            let metric_records = [];

            records.forEach((record) => {
                record._timestamp = record.time || new Date().toISOString(); // Add _timestamp for OpenObserve
                delete record.time;

                if (record && record.properties && record.properties.log) {
                    // Handle AKS logs
                    try {
                        record.properties.log = JSON.parse(record.properties.log);
                    } catch (e) {
                        context.log(`Log parsing failed, keeping raw: ${record.properties.log}`);
                    }
                    log_records[record.category] = log_records[record.category] || [];
                    log_records[record.category].push(record);
                } else if (record && record.metricName) {
                    // Handle metrics
                    metric_records.push(record);
                } else {
                    // Handle other types of logs
                    log_records['default'].push(record);
                }
            });

            try {
                // Send logs
                const log_promises = Object.keys(log_records).map((stream) => {
                    const log_url = `${org_url}/${log_stream_prefix}_${stream}/_json`;
                    return send_data(context, log_url, log_records[stream], auth);
                });
                await Promise.all(log_promises);

                // Send metrics
                if (metric_records.length > 0) {
                    await send_data(context, metric_url, metric_records, auth);
                }

                context.log("Records successfully sent.");
            } catch (error) {
                context.log("Error sending data: ", error);
            }
        }));

        context.log("All Event Hub messages processed.");
    } catch (error) {
        context.log("Unhandled exception: ", error);
    }
};

/********* Helper Function: send_data with retry logic *********/
async function send_data(context, url, records, auth) {
    if (!records || records.length === 0) {
        context.log(`No records to send to ${url}`);
        return;
    }

    const maxRetries = 5;
    let delayMs = 1000; // Initial delay in milliseconds
    const ingestion_id = uuidv4();
    url += `?ingestion_id=${ingestion_id}`;

    // Add ingestion_id to each record for debugging
    records.forEach((record) => record.ingestion_id = ingestion_id);

    const encodedCredentials = Buffer.from(`${auth.username}:${auth.password}`).toString('base64');
    const headers = {
        Authorization: `Basic ${encodedCredentials}`,
        "Content-Type": "application/json"
    };

    context.log(`Sending headers: ${JSON.stringify(headers)}`);

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            const response = await axios.post(url, records, { auth });
            context.log(`Sent ${records.length} records to ${url}. Response: ${response.status}, Duration: ${response.duration}ms`);
            return;
        } catch (error) {
            context.log(`Attempt ${attempt} failed to send data to ${url}. Error: ${error.message}`);
            if (error.response) {
                context.log(`Error response data: ${JSON.stringify(error.response.data)}`);
            }

            if (error.response && error.response.status >= 400 && error.response.status < 500) {
                context.log("Non-retriable error. Aborting further attempts.");
                break;
            }

            if (attempt < maxRetries) {
                context.log(`Retrying after ${delayMs}ms...`);
                await new Promise(resolve => setTimeout(resolve, delayMs));
                delayMs *= 2; // Exponential backoff
            } else {
                context.log("Max retries reached. Failing this batch.");
            }
        }
    }
}

