import http from "http";
import WebSocket from "ws";


const SYMBOL = "solusdt";


// Теперь уровни будут приходить через API
let LEVELS = [];


// Telegram
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT = process.env.TELEGRAM_CHAT;



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
            "Telegram status:",
            r.status
        );


    }catch(e){

        console.log(
            "Telegram error:",
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



        if(
            level.type === "below" &&
            price <= level.price &&
            !level.triggered
        ){

            level.triggered = true;


            sendTelegram(
                `SOLUSDT ниже уровня ${level.price}. Цена ${price}`
            );

        }


    }

}




// HTTP API

const server =
http.createServer(
async (req,res)=>{


    res.setHeader(
        "Content-Type",
        "application/json"
    );



    // Получить уровни
    if(
        req.method === "GET" &&
        req.url === "/api/levels"
    ){

        res.end(
            JSON.stringify(
                LEVELS
            )
        );

        return;
    }



    // Добавить уровень
    if(
        req.method === "POST" &&
        req.url === "/api/levels"
    ){


        let body="";


        req.on(
            "data",
            chunk=>{
                body += chunk;
            }
        );


        req.on(
            "end",
            ()=>{


                try{


                    const level =
                    JSON.parse(body);



                    level.triggered=false;



                    LEVELS.push(
                        level
                    );



                    console.log(
                        "NEW LEVEL:",
                        level
                    );



                    res.end(
                        JSON.stringify({
                            ok:true,
                            level
                        })
                    );


                }catch(e){


                    res.statusCode=400;


                    res.end(
                        JSON.stringify({
                            ok:false,
                            error:e.message
                        })
                    );

                }


            }
        );


        return;

    }



    // Удалить все уровни
    if(
        req.method === "DELETE" &&
        req.url === "/api/levels"
    ){

        LEVELS=[];


        res.end(
            JSON.stringify({
                ok:true
            })
        );


        return;

    }



    res.end(
        JSON.stringify({
            ok:true,
            service:"Render Binance Monitor"
        })
    );


});



server.listen(
process.env.PORT || 10000,
()=>{

    console.log(
        "HTTP server started"
    );

});




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
