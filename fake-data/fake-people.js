const { createClient } =  require('@clickhouse/client' );
const { faker } = require('@faker-js/faker');
require('dotenv').config();

const client = createClient({
  host: `http://${process.env.DB_HOST}:${process.env.DB_PORT}`, // Replace with your ClickHouse host
  user: `${process.env.DB_USER}`, // Replace with your ClickHouse username
  password: `${process.env.DB_PASSWORD}`, // Replace with your ClickHouse password
  database: `${process.env.DB_NAME}`, // Replace with your ClickHouse database name
});

async function generatePeopleData(totalPeople) {
  const peopleData = [];
  let uniqueId = generateUniqueID(13);
  for (let i = 0; i < totalPeople; i++) {
    peopleData.push({
        id: (parseInt(uniqueId)+i).toString(),
        first_name: faker.person.firstName(),
        last_name: faker.person.lastName(),
        dateOfBirth: faker.date.between({from:'1990-01-01T00:00:00.00Z', to: '2020-12-31T00:00:00.00Z'}).toISOString().substring(0,10),
        laser_id: faker.string.alphanumeric(12),
      });

  }
  return peopleData;
}

function generateUniqueID(length) {
  let result = '';
  const characters = '0123456789';
  const charactersLength = characters.length;
  for (let i = 0; i < length; i++) {
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
  }
  return result;
}

async function insertData(data) {
  await client.insert({
    table: 'people',
    values: [...data],
    format: 'JSONEachRow'
  });
}
// Starts the timer 

(async () => {
  console.time('generate data'); 
  const totalPeople = 1_000_000; // Change this to your desired number of people
  const peopleData = await generatePeopleData(totalPeople);
  console.timeEnd('generate data'); 
  console.time('insert data');
  await insertData(peopleData);
  console.timeEnd('insert data');
  console.log(`Successfully inserted ${totalPeople} people data into ClickHouse table.`);
  
})();

