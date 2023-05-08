import { faker } from '@faker-js/faker';
import { MongoClient, InsertManyResult } from 'mongodb';
import { ICustomer } from './customer.type';
import * as dotenv from 'dotenv';

dotenv.config();

const createCustomer = (): ICustomer => {
  const sex = faker.name.sex() as 'male' | 'female';
  const firstName = faker.name.firstName(sex);
  const lastName = faker.name.lastName(sex);
  const email = `${firstName}.${lastName}@${faker.internet.email().split('@')[1]}`;
  const address = {
    line1: faker.address.streetAddress(),
    line2: faker.address.secondaryAddress(),
    postcode: faker.address.zipCode('#####'),
    city: faker.address.cityName(),
    state: faker.address.stateAbbr(),
    country: faker.address.countryCode('alpha-2'),
  };
  const createdAt = new Date();
  return { firstName, lastName, email, address, createdAt };
};

const createPacketCustomers = (): ICustomer[] => {
  return Array.from({ length: Math.floor(Math.random() * 10 + 1) }).map(createCustomer);
};

async function createCustomersDb(client: MongoClient, newPacket: ICustomer[]): Promise<void> {
  const result: InsertManyResult = await client
    .db('test')
    .collection('customers')
    .insertMany(newPacket);
  console.log(`${result.insertedCount} new customers created with the following ids: `, result);
}

async function main(): Promise<void> {
  const uri = process.env.DB_URI as string;
  const client: MongoClient = new MongoClient(uri);
  let intervalID: string | number | NodeJS.Timeout | undefined;
  try {
    await client.connect();
    intervalID = setInterval(() => {
      const newCustomers = createPacketCustomers();
      console.log('newCustomers:', newCustomers);
      createCustomersDb(client, newCustomers);
    }, 200);
  } catch (err) {
    console.error(err);
    clearInterval(intervalID);
    await client.close();
  }
}

main().catch(console.error);
