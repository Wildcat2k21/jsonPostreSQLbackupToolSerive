const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

//установки сервера
const app = express();
const PORT = process.env.PORT || 3000;

// Разрешить все источники
app.use(cors());

//использование json в ответах
app.use(express.json());

//объявляем пул, чтобы он был доступен в приложении после создания подключения в createConnection
let client, customPool;

// Обработчик для создания подключения
app.post('/createConnection', async (req, res) => {
    try {

        // Если подключение уже существует, закрываем его перед созданием нового
        if (customPool) await closePool();

        // Получаем данные для подключения из запроса (например, req.body)
        const dbConnectionIfgo = req.body;

        // Создаем новый пул подключений с переданными данными
        customPool = new Pool(dbConnectionIfgo);

        // Сохраняем подключение в пуле
        client = await customPool.connect();

        res.status(200).send('connection saved');

    } 
    //обработка ошибок
    catch(err) {

        //сообщения
        console.error('unexpected error:', err);
        res.status(500).send(err.message);

        //очистка переменных (для новой попытки)
        client = null;
        customPool = null;
    }
});

//восстановление таблиц
app.post('/restoreTables', async (req, res) => {
    try{

        //проверка, что подключение было создано
        if(!customPool){
            return res.status(404).send('needs connection');
        }

        const sql = req.body.sql;

        //выполнение sql запроса
        const result = await customPool.query(sql);
        res.status(200).send('OK');

    }catch(err){
        console.error('Error creating tables: ', err);
        res.status(500).send(err.message);
    }
});

//данные, ожидающие сохранения
let restoreData = [];

//восстановление данных
app.post('/restoreData', async (req, res) => {
    try{

        //проверка, что подключение было создано
        if(!customPool){
            return res.status(404).send('needs connection');
        }

        // jsonStrPart, isEnd, index, lastIndex
        const dataPart = req.body;

        //очистка массива с данными
        if(dataPart.index === 0 && restoreData.length){
            restoreData.length = 0;
        }

        let data;

        //добавление данных
        if(!dataPart.isEnd){
            restoreData.push(dataPart.jsonStrPart);
            return res.status(200).send('OK');
        }
        else {
            restoreData.push(dataPart.jsonStrPart);
            const fullDataString = restoreData.join('');
            restoreData = [];
            data = JSON.parse(fullDataString);
        }

        //нужные таблицы
        const userTable = data.find(table => table.name === 'plugin_user');
        const subjectTable = data.find(table => table.name === 'subject');
        const testTable = data.find(table => table.name === 'test');
        const questionTable = data.find(table => table.name === 'question');
        const qestImageTable = data.find(table => table.name === 'question_image');
        const answerTable = data.find(table => table.name === 'answer');
        const ansImageTable = data.find(table => table.name === 'answer_image');

        //вставка записей в нужной последовательности
        await insertInTable(userTable);
        await insertInTable(subjectTable);
        await insertInTable(testTable);
        await insertInTable(questionTable);
        await insertInTable(qestImageTable);
        await insertInTable(answerTable);
        await insertInTable(ansImageTable);

        //успех
        res.status(200).send('OK');

    }catch(err){
        console.error('Error pushing data:', err);
        restoreData = [];
        res.status(500).send(err.message);
    }
});

//закрытие пул соединения
app.post('/closeConnection', async (req, res) => {
    try{
        if(customPool) await closePool();
        res.status(200).send('OK');

    }catch(err){
        console.error(err);
        res.status(500).send(err.message);
    }
});

//обновление счетчика id у таблицы
async function restartTableSequence(tableName){
    const countResult = await customPool.query(`SELECT COUNT(*) FROM ${tableName}`);
    const nextval = Number(countResult.rows[0].count) + 1;
    const sql = `ALTER SEQUENCE ${tableName}_id_seq RESTART WITH ${nextval};`
    // const sql = `DROP SEQUENCE ${tableName}_id_seq CASCADE; CREATE SEQUENCE ${tableName}_id_seq START ${nextval};`;
    await customPool.query(sql);
}

//закрытие подключения
async function closePool(){
    await client.release();
    await customPool.end();

    //очистка переменных
    client = null;
    customPool = null;
}

//вставка данных в таблицы
async function insertInTable(table){
    const entries = table.content;
    //вывод данных
    for(let entry of entries){
        const {insertSql, values} = createInsertCommand(table.name, entry);
        await customPool.query(insertSql, values)
    }

    //обновление счетчика у таблицы
    await restartTableSequence(table.name);
}

//создание резервной копии таблицы
app.post('/createTableBackup', async (req, res) => {
    try {

        //проверка, что подключение было создано
        if(!customPool){
            return res.status(404).send('needs connection');
        }

        //получение названия таблицы
        const tableName = req.body.name;

        //проверка на бинарные тип файла
        const isBinary = (value) => typeof value === 'object' && value instanceof Buffer;

        // Используем параметризованный запрос для предотвращения SQL-инъекций
        const result = await customPool.query(`SELECT * FROM ${tableName}`);

        // Предположим, что result.rows содержит ваши данные
        const base64rows = result.rows.map((row) => {
            for(const field in row){
                if(isBinary(row[field])){
                    row[field] = Buffer.from(row[field], 'binary').toString('base64');
                }
            }

            return row;
        });

        // преобразовать в строку
        const jsonDataString = JSON.stringify(base64rows, null, 2);

        // запрос чанками
        await parseRequestText(jsonDataString, 1000, async (jsonStrPart, isEnd) => {
            //отправка данных потоком
            res.write(jsonStrPart);
            //если данные переданы, завершить
            if(isEnd) res.end();
        });

    } 
    //обработка ошибок
    catch(err) {
        //таблица не найдена
        if(err.code === '42P01'){
            return res.status(404).send('table with that name not exists');
        } 

        //если ошибка не связана с поиском таблицы
        console.error('creating reserve copy error:', err);
        return res.status(500).send(err.message);
    }
});

//создание команды для вставки
function createInsertCommand(name, entry){
    const fields = Object.keys(entry);
    const numbers = Object.keys(entry).map((key, n) => `$${n + 1}`);
    const insertSql = `INSERT INTO ${name} (${fields.join(', ')}) VALUES (${numbers.join(', ')}) RETURNING *;`;
    //создание значений для вставки
    const values = fields.map(key => {
        let value = entry[key];
        if((name === 'question_image' && key === 'image')||(name === 'answer_image' && key === 'image') ){
            value = Buffer.from(value, 'base64');
        }

        return value;
    });

    return {insertSql, values}
}

//разбитие строки json на подстроки для запроса порциями
async function parseRequestText(jsonString, length, callback){

    //инициализация
    let startAt = 0;
    let isEnd = false;
    let lastIndex = Math.floor(jsonString.length/length);
  
  //следующая часть
  async function next(start){
    if(start + length >= jsonString.length){
      isEnd = true;
    }
    //текущая часть
    let jsonStrPart = jsonString.slice(start, start + length);
        //текущий индекс
        let index = start/length;
    //функция callback
        await callback(jsonStrPart, isEnd, index, lastIndex);
    //проверка
    if(start + length < jsonString.length){
          //следующий вызов
          await next(start + length);
    }
  }
  
    //следующий индекс
  await next(startAt);
}

//запуск сервера
app.listen(PORT, () => {
    console.log(`server started on port: ${PORT}`);
});