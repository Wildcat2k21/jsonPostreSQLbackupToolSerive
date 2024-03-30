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
        if (customPool){
            await client.release();
            await customPool.end();

            //очистка переменных
            client = null;
            customPool = null;
        }

        // Получаем данные для подключения из запроса (например, req.body)
        const dbConnectionIfgo = req.body;

        // Создаем новый пул подключений с переданными данными
        customPool = new Pool(dbConnectionIfgo);

        // Сохраняем подключение в пуле
        client = await customPool.connect();

        res.status(200).send('connection saved');

    } catch (error) {
        console.error('unexpected error:', error);
        res.status(500).send('db connection error');
    }
});

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
    catch (error) {

        //если ошибка не связана с поиском таблицы
        if(error.code !== '42P01'){
            console.error('creating reserve copy error:', error);
            return res.status(500).send('Creating reserve copy failed');
        } 

        //таблица не найдена
        const response = {
            status: 404,
            message: 'table with that name not exists'
        }

        //если такой таблицы нет
        res.json(response);
    }
});

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