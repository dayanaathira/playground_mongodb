const { MongoClient, ObjectId } = require('mongodb');
const fs = require('node:fs');
const csv = require('fast-csv');
const logger = require('./util/logger');

async function cleanupAssets() {
    const uri = 'mongodb://127.0.0.1:27017';
    const client = new MongoClient(uri);

    try {
        await client.connect();
        logger.info('Connected to the database');

        const database = client.db('staging');
        const assetDescCollection = database.collection('asset_desc');
        const assetCollection = database.collection('assets');


        // Get all documents in asset_desc & assets collection
        const assetDescDocuments = await assetDescCollection.find({ delete_flag: { $exists: false } }).toArray();
        // const assetDocuments = await assetCollection.find({ delete_flag: { $exists: false } }).toArray();

        const nameObj = {};

        for (const doc of assetDescDocuments) {
            const model = doc.model;
            const doc_id = doc._id;
            const asset_type = doc.asset_type;
            const asset_type_desc = doc.asset_type_desc;

            try {
                // find and delete tenant_id
                const rmTenantId = await assetDescCollection.findOneAndUpdate({ _id: new ObjectId(doc_id) }, { $unset: { tenant_id: 1 } });
                logger.info(`[asset_desc collection] Successfully remove tenant_id`);
            } catch (error) {
                logger.error(`[asset_desc collection] Error rm tenant_id: ${error}`);
            }

            if (model in nameObj) {
                if (asset_type === nameObj[model].a_type && asset_type_desc === nameObj[model].a_type_desc) {
                    // lookup asset to check if that asset_desc_id being used at any document
                    try {
                        // lookup asset_desc_id and replace asset_desc_id to new
                        const a_desc_id = await assetCollection.updateMany({ asset_desc_id: doc_id.toString() }, { $set: { asset_desc_id: nameObj[model].id.toString() } });
                        logger.info(`[assets collection] Successfully update asset_desc_id`);
                    } catch (error) {
                        logger.error(`[assets collection] Error to update asset_desc_id: ${error}`);
                    }
                    try {
                        // findbyId and delete the documents for asset_desc of duplicate
                        const rmDocument = await assetDescCollection.findOneAndDelete({ _id: new ObjectId(doc_id) });
                        logger.info(`[asset_desc collection] Successfully remove duplicate document`);
                    } catch (error) {
                        logger.error(`[asset_desc collection] Error rm duplicate document: ${error}`);
                    }
                } else {
                    // Create a new entry in nameObj
                    nameObj[model] = {
                        model_name: model,
                        id: doc_id,
                        a_type: asset_type,
                        a_type_desc: asset_type_desc,
                    };
                }
            } else {
                // Create a new entry in nameObj
                nameObj[model] = {
                    model_name: model,
                    id: doc_id,
                    a_type: asset_type,
                    a_type_desc: asset_type_desc,
                };
            }
        }

        // Retrieve cleanup data
        const c_assets = await assetCollection.find({ delete_flag: { $exists: false } }).toArray();
        const c_asset_desc = await assetDescCollection.find({ delete_flag: { $exists: false } }).toArray();

        let collections = [c_assets, c_asset_desc];
        // let jsonString = {};
        try { 
            
            await fs.writeFileSync('assets_staging.json', '');
            await fs.writeFileSync('assets_desc_staging.json', '');

            collections.forEach(async x => {
                for (const data of x) {
                    if (x === c_assets && data._id) {
                        var result = await process(data);
                        await fs.appendFileSync(`assets_staging.json`, result + ',\n');
                        logger.info(`[Write file data sets] Successfully write file`);
                    }
                    
                    if (x === c_asset_desc && data._id){
                        var result = await process(data);
                        await fs.appendFileSync(`assets_desc_staging.json`, result + ',\n');
                        logger.info(`[Write file data sets] Successfully write file`);
                    }
                    
                }
            })

        } catch (error) {
            logger.error(`[Error write file]: ${error}`);
        }

    } finally {
        await client.close();
    }
}

async function pattern(jsonString) {
    let updJson;

    const patternsAndReplacements = [
        { pattern: /"_id": "ObjectId\('(\w+)'\)"/g, replacement: '"_id": ObjectId(\'$1\')' },
        { pattern: /"updatedAt": "(ISODate\('\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z'\))"/g, replacement: '"updatedAt": $1' },
        { pattern: /"createdAt": "(ISODate\('\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z'\))"/g, replacement: '"createdAt": $1' },
    ];
    updJson = jsonString;
    patternsAndReplacements.forEach(({ pattern, replacement }) => {
        updJson = updJson.replace(pattern, replacement);
    });

    return updJson;
}

async function process(data){
    if (data.createdAt && data.updatedAt) {
        // to set _id, createdAt and updateAt based on mongo express
        jsonString = JSON.stringify({
            ...data,
            "_id": "ObjectId('" + new ObjectId(data._id) + "')",
            "createdAt": "ISODate('" + data.createdAt.toISOString() + "')",
            "updatedAt": "ISODate('" + data.updatedAt.toISOString() + "')"
        }, null, 2);

        try {
            // Define an array of patterns and their replacements
           var result = await pattern(jsonString);

        } catch (error) {
            logger.error(`Can not replace _id, createdAt and updatedAt to the dessire pattern: ${error}`);
        }

    } else {
        if (data.createdAt && !data.updatedAt) {
            // to set _id, createdAt and updateAt based on mongo express
            jsonString = JSON.stringify({
                ...data,
                "_id": "ObjectId('" + new ObjectId(data._id) + "')",
                "createdAt": "ISODate('" + data.createdAt.toISOString() + "')"
            }, null, 2);

            try {
                // Define an array of patterns and their replacements
                var result = await pattern(jsonString);

            } catch (error) {
                logger.error(`Can not replace _id and createdAt to the dessire pattern: ${error}`);
            }
        } else if (data.updatedAt && !data.createdAt) {
            // to set _id, createdAt and updateAt based on mongo express
            jsonString = JSON.stringify({
                ...data,
                "_id": "ObjectId('" + new ObjectId(data._id) + "')",
                "updatedAt": "ISODate('" + data.updatedAt.toISOString() + "')"
            }, null, 2);

            try {
                // Define an array of patterns and their replacements
                var result = await pattern(jsonString);

            } catch (error) {
                logger.error(`Can not replace _id and updatedAt to the dessire pattern: ${error}`);
            }
        } else {
            // to set _id, createdAt and updateAt based on mongo express
            jsonString = JSON.stringify({
                ...data,
                "_id": "ObjectId('" + new ObjectId(data._id) + "')",
            }, null, 2);

            try {
                // To change pattern for _id field from string to not string
                const currPattern = /"_id": "ObjectId\('(\w+)'\)"/g;
                var result = jsonString.replace(currPattern, '"_id": ObjectId(\'$1\')');

            } catch (error) {
                logger.error(`Can not replace _id to the dessire pattern: ${error}`);
            }
        }
    }

    return result;
}

// Run the main function
cleanupAssets();
