import http from "http";
import WebSocket from "ws";


const SYMBOL = "solusdt";

const LEVELS = [
    {
        symbol: "SOLUSDT",
        price: 100,
        type: "above",
        triggered: false
    }
];


// Render Environment Variables
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT = process.env.TELEGRAM_CHAT;



console.log(
    "Telegram variables:",
    {
        token_exists: !!TELEGRAM_TOKEN,
        chat_exists: !!TELEGRAM_CHAT
    }
);



async function testTelegramConnection(){

    try{

        const r = await fetch(
            "https://api.telegram.org"
        );

        console.log(
            "Telegram connection status:",
            r.status
        );


    }catch(e){

        console.log(
            "Telegram connection error:",
            e.message
        );

    }

}



async function testTelegramBot(){

    if(!TELEGRAM_TOKEN){

        console.log(
            "Telegram token missing"
        );

        return;
    }


    try{

        const url =
        `https://api.telegram.org/bot${TELEGRAM_TOKEN}/getMe`;


        const r = await fetch(url);


        console.log(
            "Telegram getMe status:",
            r.status
        );


        console.log(
            await r.text()
        );


    }catch(e){

        console.log(
            "Telegram getMe error:",
            e.message
        );

    }

}



async function sendTelegram(text){


    console.log(
        "TELEGRAM MESSAGE:",
        text
    );


    if(!TELEGRAM_TOKEN || !TELEGRAM_CHAT){

        console.log(
            "Telegram disabled"
        );

        return;
    }



    try{


        const url =
        `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`;


        const r = await fetch(
            url,
            {
                method:"POST",
                headers:{
                    "Content-Type":"application/json"
                },
                body:JSON.stringify({

                    chat_id: TELEGRAM_CHAT,
                    text:text

                })
            }
        );


        console.log(
            "Telegram send status:",
            r.status
        );


        console.log(
            await r.text()
        );


    }catch(e){

        console.log(
            "Telegram send error:",
            e.message
        );

    }


}



function checkLevels(price){


    for(const level of LEVELS){


        if(level.symbol !== "SOLUSDT")
            continue;



        if(
            level.type === "above" &&
            price >= level.price &&
            !level.triggered
        ){

            level.triggered = true;


            sendTelegram(
                `SOLUSDT пробил уровень ${level.price}. Цена ${price}`
            );


        }


    }


}




const server =
http.createServer(
(req,res)=>{

    res.writeHead(200);

    res.end(
        "Render Binance Monitor OK"
    );

});



server.listen(
process.env.PORT || 10000,
()=>{

    console.log(
        "HTTP server started"
    );

});



// Telegram diagnostics
testTelegramConnection();
testTelegramBot();




// Binance WebSocket

const url =
`wss://fstream.binance.com/market/stream?streams=${SYMBOL}@aggTrade`;


console.log(
"Connecting Binance:",
url
);



const ws =
new WebSocket(url);



ws.on(
"open",
()=>{

    console.log(
        "BINANCE CONNECTED"
    );

});



ws.on(
"message",
(data)=>{


    const msg =
    JSON.parse(data);


    const trade =
    msg.data;


    const price =
    Number(trade.p);



    console.log(
        SYMBOL,
        price
    );



    checkLevels(price);


});



ws.on(
"error",
(err)=>{

    console.log(
        "WS ERROR",
        err.message
    );

});



ws.on(
"close",
()=>{

    console.log(
        "WS CLOSED"
    );

});
