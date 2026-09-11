import http from "http";
import WebSocket from "ws";
import { createClient } from "@supabase/supabase-js";


const SYMBOL = "solusdt";


// ===============================
// SUPABASE
// ===============================

const supabase =
createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);


console.log(
    "SUPABASE CHECK:",
    {
        url: !!process.env.SUPABASE_URL,
        key: !!process.env.SUPABASE_SERVICE_KEY
    }
);



// ===============================
// TELEGRAM
// ===============================

const TELEGRAM_TOKEN =
process.env.TELEGRAM_TOKEN;


const TELEGRAM_CHAT =
process.env.TELEGRAM_CHAT;



async function sendTelegram(text){


    console.log(
        "TELEGRAM MESSAGE:",
        text
    );


    if(
        !TELEGRAM_TOKEN ||
        !TELEGRAM_CHAT
    ){
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

                    chat_id:TELEGRAM_CHAT,
                    text:text

                })
            }
        );


        console.log(
            "Telegram status:",
            response.status
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


    const {
        data,
        error
    } =
    await supabase
    .from("levels")
    .select("*");



    if(error){

        console.log(
            "SUPABASE ERROR:",
            error.message
        );

        return [];

    }


    return data || [];

}




// ===============================
// ALERT CHECK
// ===============================

async function checkLevels(
    price,
    receivedAt
){


    const levels =
    await getLevels();



    for(
        const level of levels
    ){


        if(
            level.symbol !== "SOLUSDT"
        ){
            continue;
        }



        let triggered=false;



        if(
            level.type==="above" &&
            price >= Number(level.price) &&
            level.active
        ){

            triggered=true;

        }



        if(
            level.type==="below" &&
            price <= Number(level.price) &&
            level.active
        ){

            triggered=true;

        }



        if(triggered){


            const now =
            new Date();



            const localTime =
            now.toLocaleTimeString(
                "ru-RU",
                {
                    hour12:false
                }
            );



            const latency =
            Date.now() - receivedAt;



            const alertName =
            level.alert_name ||
            "Сигнальный уровень";



            const exchange =
            level.exchange ||
            "Binance Futures";



            let message =
`🔔 ${alertName}

${exchange}
${level.symbol}

Цена: ${price}`;



            if(level.price){

                message +=
`\nУровень: ${level.price}`;

            }



            message +=
`

Время: ${localTime}
Задержка: ${latency} мс`;



            await sendTelegram(
                message
            );



            await supabase
            .from("levels")
            .update({

                active:false

            })
            .eq(
                "id",
                level.id
            );


        }


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
            c=>body+=c
        );


        req.on(
            "end",
            async()=>{


                const data =
                JSON.parse(body);



                const level={


                    symbol:data.symbol,

                    price:Number(
                        data.price
                    ),

                    type:data.type,

                    active:true,


                    alert_name:
                    data.alert_name ||
                    "Сигнальный уровень",


                    exchange:
                    data.exchange ||
                    "Binance Futures"


                };



                const {
                    data:created,
                    error
                } =
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
            service:
            "Binance Monitor GS"

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




// ===============================
// BINANCE WS
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


    const receivedAt =
    Date.now();



    const msg =
    JSON.parse(data);



    const price =
    Number(
        msg.data.p
    );



    console.log(
        SYMBOL,
        price
    );



    await checkLevels(
        price,
        receivedAt
    );


});



ws.on(
"error",
e=>{

console.log(
"WS ERROR:",
e.message
);

});
