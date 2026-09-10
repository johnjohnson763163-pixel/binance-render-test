import http from "http";
import WebSocket from "ws";


const SYMBOL = "solusdt";

const LEVELS = [
    {
        symbol: "SOLUSDT",
        price: 101.20,
        type: "above",
        triggered: false
    }
];


const TELEGRAM_TOKEN = "ВСТАВИМ_ПОТОМ";
const TELEGRAM_CHAT = "ВСТАВИМ_ПОТОМ";


function sendTelegram(text){

    console.log("TELEGRAM:", text);

    /*
    Потом включим:

    fetch(
      `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`,
      {
        method:"POST",
        headers:{
          "Content-Type":"application/json"
        },
        body:JSON.stringify({
          chat_id:TELEGRAM_CHAT,
          text:text
        })
      }
    );
    */
}



function checkLevels(price){

    for(const level of LEVELS){

        if(level.symbol !== "SOLUSDT")
            continue;


        if(
            level.type==="above" &&
            price >= level.price
        ){

            sendTelegram(
                `SOLUSDT пробил уровень ${level.price}. Цена ${price}`
            );

        }


        if(
            level.type==="below" &&
            price <= level.price
        ){

            sendTelegram(
                `SOLUSDT ниже уровня ${level.price}. Цена ${price}`
            );

        }

    }

}



const server=http.createServer(
(req,res)=>{

    res.writeHead(200);
    res.end("Render Binance Monitor OK");

});


server.listen(
process.env.PORT || 10000,
()=>{
console.log(
"HTTP server started"
);
}
);



const url =
`wss://fstream.binance.com/market/stream?streams=${SYMBOL}@aggTrade`;


console.log(
"Connecting Binance:",
url
);


const ws=new WebSocket(url);


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

    const msg=
    JSON.parse(data);


    const trade=
    msg.data;


    const price=
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
