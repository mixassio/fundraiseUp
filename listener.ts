import { ChangeStreamInsertDocument, ChangeStreamUpdateDocument, MongoClient } from 'mongodb';
import * as dotenv from 'dotenv';
import { ICustomer } from './customer.type';
import * as hash from 'custom-hash';

dotenv.config();
hash.configure({
  charSet: process.env.CHAR_SET?.split(''),
  maxLength: Number(process.env.LENGTH_HASH),
});

const anonimizeCustomer = (customer: ICustomer): ICustomer => {
  const {
    firstName,
    lastName,
    email,
    address: { line1, line2, postcode, ...addressTail },
    ...tail
  } = customer;
  return {
    firstName: hash.digest(firstName),
    lastName: hash.digest(lastName),
    email: `${hash.digest(firstName + lastName)}@${email.split('@')[1]}`,
    address: {
      line1: hash.digest(line1),
      line2: hash.digest(line2),
      postcode: hash.digest(postcode),
      ...addressTail,
    },
    ...tail,
  };
};

const anonimizeFields = [
  'firstName',
  'lastName',
  'email',
  'address.line1',
  'address.line2',
  'postcode',
];

const createData = (
  next: ChangeStreamInsertDocument | ChangeStreamUpdateDocument,
): { type: 'update' | 'insert'; data: ICustomer | object } => {
  if (next.operationType === 'insert') {
    return {
      type: 'insert',
      data: anonimizeCustomer(next.fullDocument as ICustomer),
    };
  }
  if (next.operationType === 'update') {
    const result = {};
    Object.entries(next.updateDescription.updatedFields as Partial<Document>).map(
      ([key, value]) => {
        result[key] = anonimizeFields.includes(key) ? hash.digest(value) : value;
      },
    );
    return {
      type: 'update',
      data: { _id: next.documentKey._id, ...result },
    };
  }
  throw new Error('Handler for operation not exist');
};

const addNewCustomers = (client: MongoClient, newCustomersCache): void => {
  const qwery = newCustomersCache.map(({ type, data }) => {
    if (type === 'insert') {
      return { insertOne: { document: data } };
    }
    if (type === 'update') {
      return {
        updateOne: {
          filter: { _id: data._id },
          update: { $set: { ...data } },
          upsert: true,
        },
      };
    }
  });
  newCustomersCache.length = 0;
  client.db('test').collection('customers_anonymised').bulkWrite(qwery);
};

async function monitor(client: MongoClient): Promise<void> {
  const newCustomersCache: { type: 'update' | 'insert'; data: ICustomer | object }[] = [];
  const collection = client.db('test').collection('customers');
  const changeStream = collection.watch([
    {
      $match: {
        $or: [{ operationType: 'insert' }, { operationType: 'update' }],
      },
    },
  ]);

  setInterval(() => {
    console.log('newCustomersCache.length->', newCustomersCache.length);
    if (newCustomersCache.length > 0) {
      addNewCustomers(client, newCustomersCache);
    }
  }, 1000);

  changeStream.on('change', (next: ChangeStreamInsertDocument | ChangeStreamUpdateDocument) => {
    newCustomersCache.push(createData(next));
    if (newCustomersCache.length === 1000) {
      addNewCustomers(client, newCustomersCache);
    }
  });
}

async function sinhronize(client: MongoClient, lastDate?): Promise<void> {
  const docs = await client
    .db('test')
    .collection('customers')
    .find({ ...lastDate })
    .toArray();
  const qwery = docs.map(({ _id, ...customer }) => {
    return {
      updateOne: {
        filter: { _id },
        update: { $set: { ...anonimizeCustomer(customer as ICustomer) } },
        upsert: true,
      },
    };
  });
  if (qwery.length > 0) {
    await client.db('test').collection('customers_anonymised').bulkWrite(qwery);
  }
}

async function sinhronizeLastTime(client: MongoClient): Promise<void> {
  const docs = await client
    .db('test')
    .collection('customers_anonymised')
    .find()
    .sort({ createdAt: -1 })
    .limit(1)
    .toArray();
  const lastDate = Array.isArray(docs) && docs.length > 0 ? docs[0].createdAt : null;
  if (lastDate) {
    await sinhronize(client, { createdAt: { $gt: lastDate } });
  }
}

async function main(): Promise<void> {
  const uri = process.env.DB_URI as string;
  const client: MongoClient = new MongoClient(uri);
  try {
    await client.connect();

    if (process.argv.includes('--full-reindex')) {
      await sinhronize(client);
      process.exit(0);
    }
    await sinhronizeLastTime(client);
    await monitor(client);
  } catch (e) {
    console.log(e);
    await client.close();
  }
}

main().catch(console.error);
