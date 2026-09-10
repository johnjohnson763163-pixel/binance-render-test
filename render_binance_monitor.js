import http from "http";
import WebSocket from "ws";


// =============================
// SETTINGS
// =============================

const SYMBOL = "solusdt";


// Тестовый уровень
const LEVELS = [
    {
        symbol: "SOLUSDT",
        price: 101.20,
        type: "above",
        triggered: false
    }
];


// Telegram
// пока оставляем пустым
// потом вставим реальные значения

const TELEGRAM_TOKEN = "ВСТАВИМ_ПОТОМ";
const TELEGRAM_CHAT = "ВСТАВИМ_ПОТОМ";



// =============================
// TELEGRAM
// =============================

async function sendTelegram(text){

    console.log(
        "TELEGRAM:",
        text
    );


    if(
        TELEGRAM_TOKEN === "ВСТАВИМ_ПОТОМ" ||
        TELEGRAM_CHAT === "ВСТАВИМ_ПОТОМ"
    ){

        console.log(
            "Telegram disabled"
        );

        return;
    }


    try{


        const response =
        await fetch(
            `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`,
            {

                method:"POST",

                headers:{
                    "Content-Type":"application/json"
                },


                body:JSON.stringify({

                    chat_id:
                    TELEGRAM_CHAT,

                    text:text

                })

            }
        );


        console.log(
            "Telegram status:",
            response.status
        );


    }
    catch(error){

        console.log(
            "Telegram error:",
            error.message
        );

    }

}




// =============================
// LEVEL CHECK
// =============================


function checkLevels(price){


    for(
        const level of LEVELS
    ){


        if(
            level.symbol !== "SOLUSDT"
        )
            continue;



        if(
            level.triggered
        )
            continue;



        if(
            level.type === "above" &&
            price >= level.price
        ){

            level.triggered = true;


            sendTelegram(
                `SOLUSDT пробил уровень ${level.price}. Цена ${price}`
            );


        }



        if(
            level.type === "below" &&
            price <= level.price
        ){

            level.triggered = true;


            sendTelegram(
                `SOLUSDT ниже уровня ${level.price}. Цена ${price}`
            );


        }


    }


}




// =============================
// HTTP SERVER FOR RENDER
// =============================


const server =
http.createServer(
(req,res)=>{


    res.writeHead(
        200,
        {
            "Content-Type":
            "text/plain"
        }
    );


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

    }

);




// =============================
// BINANCE WEBSOCKET
// =============================


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


    try{


        const msg =
        JSON.parse(data);



        const trade =
        msg.data;



        const price =
        Number(
            trade.p
        );



        console.log(
            SYMBOL,
            price
        );



        checkLevels(
            price
        );


    }

    catch(error){


        console.log(
            "Parse error:",
            error.message
        );


    }


});





ws.on(
"error",
(error)=>{


    console.log(
        "WS ERROR:",
        error.message
    );


});





ws.on(
"close",
()=>{


    console.log(
        "BINANCE CLOSED"
    );


});
