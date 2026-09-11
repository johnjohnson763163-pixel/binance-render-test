import http from "http";
import WebSocket from "ws";
import { createClient } from "@supabase/supabase-js";

const SYMBOL = "solusdt";


// ===============================
// SUPABASE
// ===============================

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);


console.log("SUPABASE CHECK:", {
    url: !!process.env.SUPABASE_URL,
    key: !!process.env.SUPABASE_SERVICE_KEY
});


// ===============================
// TELEGRAM
// ===============================

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT = process.env.TELEGRAM_CHAT;



async function sendTelegram(text){

    console.log(
        "TELEGRAM MESSAGE:",
        text
    );


    if(!TELEGRAM_TOKEN || !TELEGRAM_CHAT)
        return;


    try{

        await fetch(
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

    }
    catch(e){

        console.log(
            "Telegram error:",
            e.message
        );

    }

}



// ===============================
// GET LEVELS
// ===============================

async function getLevels(){

    const {data,error}=await supabase
        .from("levels")
        .select("*")
        .eq("triggered",false);


    if(error){

        console.log(
            "SUPABASE READ ERROR:",
            error.message
        );

        return [];

    }


    return data || [];

}



// ===============================
// TIME FORMAT
// ===============================

function localTime(){

    return new Date()
        .toLocaleString(
            "ru-RU",
            {
                hour:"2-digit",
                minute:"2-digit",
                second:"2-digit"
            }
        );

}



// ===============================
// CHECK ALERTS
// ===============================

async function checkLevels(price){


    const levels = await getLevels();



    for(const level of levels){


        if(
            level.symbol !== "SOLUSDT"
        )
        continue;



        let hit=false;



        if(
            level.condition==="above" &&
            price >= Number(level.price)
        ){

            hit=true;

        }



        if(
            level.condition==="below" &&
            price <= Number(level.price)
        ){

            hit=true;

        }



        if(!hit)
            continue;



        const now = Date.now();



        if(
            level.last_trigger_time
        ){

            const last =
            new Date(level.last_trigger_time)
            .getTime();



            const cooldown =
            Number(level.cooldown_seconds || 60)
            *1000;



            if(
                now-last < cooldown
            ){

                continue;

            }

        }



        const title =
        level.alert_name ||
        "Сигнал";



        const instrument =
        level.instrument ||
        "B-F";



        const message =

`🔔 ${title} · ${level.symbol} · ${instrument}

Цена: ${price}

Время: ${localTime()}`;



        await sendTelegram(message);



        await supabase
        .from("levels")
        .update({

            last_trigger_price:price,

            last_trigger_time:new Date()
                .toISOString()

        })
        .eq(
            "id",
            level.id
        );


    }


}



// ===============================
// HTTP SERVER
// ===============================

const server =
http.createServer(
async(req,res)=>{


    res.setHeader(
        "Content-Type",
        "application/json"
    );



    const url =
    new URL(
        req.url,
        "http://localhost"
    );



    if(
        req.method==="GET" &&
        url.pathname==="/api/levels"
    ){

        const levels =
        await getLevels();


        res.end(
            JSON.stringify(levels)
        );


        return;

    }




    if(
        req.method==="POST" &&
        url.pathname==="/api/levels"
    ){


        let body="";


        req.on(
            "data",
            chunk=>body+=chunk
        );



        req.on(
            "end",
            async()=>{


                const data =
                JSON.parse(body);



                const level={


                    symbol:data.symbol,

                    price:Number(data.price),

                    type:data.type,

                    condition:
                    data.condition ||
                    data.type,


                    active:true,

                    alert_name:
                    data.alert_name ||
                    "Сигнальный уровень",


                    exchange:
                    data.exchange ||
                    "Binance",


                    market:
                    data.market ||
                    "Futures",


                    instrument:
                    data.instrument ||
                    "B-F",


                    alert_type:
                    data.alert_type ||
                    "level",


                    triggered:false

                };



                const {data:created,error}=

                await supabase
                .from("levels")
                .insert(level)
                .select()
                .single();



                if(error){

                    res.end(
                        JSON.stringify({
                            ok:false,
                            error:error.message
                        })
                    );

                    return;

                }



                console.log(
                    "LEVEL CREATED",
                    created
                );



                res.end(
                    JSON.stringify({
                        ok:true,
                        level:created
                    })
                );


            }
        );


        return;

    }





    res.end(
        JSON.stringify({
            ok:true,
            service:"Render Binance Monitor GS"
        })
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



// ===============================
// BINANCE WEBSOCKET
// ===============================

const ws =
new WebSocket(
`wss://fstream.binance.com/market/stream?streams=${SYMBOL}@aggTrade`
);



ws.on(
"open",
()=>{

console.log(
"BINANCE CONNECTED"
);

});



ws.on(
"message",
async(data)=>{


    const msg =
    JSON.parse(data);



    const price =
    Number(msg.data.p);



    console.log(
        SYMBOL,
        price
    );



    await checkLevels(price);


});



ws.on(
"error",
e=>{

console.log(
"WS ERROR:",
e.message
);

});
