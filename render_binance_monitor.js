import http from "http";
import WebSocket from "ws";
import { createClient } from "@supabase/supabase-js";


// ===============================
// CONFIG
// ===============================

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

const TELEGRAM_TOKEN =
process.env.TELEGRAM_TOKEN;

const TELEGRAM_CHAT =
process.env.TELEGRAM_CHAT;



async function sendTelegram(text){

    console.log(
        "TELEGRAM MESSAGE:",
        text
    );


    if(!TELEGRAM_TOKEN || !TELEGRAM_CHAT)
        return;


    const r = await fetch(
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
        r.status
    );

}



// ===============================
// DATABASE
// ===============================

async function getLevels(){

    const {data,error}=await supabase
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
// LEVEL CHECK
// ===============================

async function checkLevels(price){


    const levels =
    await getLevels();



    for(const level of levels){


        if(level.symbol !== "SOLUSDT")
            continue;



        if(
            level.type==="above" &&
            price >= Number(level.price) &&
            level.active
        ){


            await sendTelegram(
                `${level.symbol} пробил уровень ${level.price}. Цена ${price}`
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



        if(
            level.type==="below" &&
            price <= Number(level.price) &&
            level.active
        ){


            await sendTelegram(
                `${level.symbol} ниже уровня ${level.price}. Цена ${price}`
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
// HTTP API
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



    // GET LEVELS

    if(
        req.method==="GET" &&
        url.pathname==="/api/levels"
    ){

        res.end(
            JSON.stringify(
                await getLevels()
            )
        );

        return;
    }




    // CREATE LEVEL

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


                const d =
                JSON.parse(body);



                const {data,error}=

                await supabase
                .from("levels")
                .insert({

                    symbol:d.symbol.toUpperCase(),
                    price:Number(d.price),
                    type:d.type,
                    active:true

                })
                .select()
                .single();



                res.end(
                    JSON.stringify({
                        ok:!error,
                        level:data,
                        error:error?.message
                    })
                );


            }
        );


        return;

    }





    // UPDATE LEVEL

    if(
        req.method==="PUT" &&
        url.pathname.startsWith("/api/levels/")
    ){

        const id =
        Number(
            url.pathname.split("/").pop()
        );


        let body="";


        req.on(
            "data",
            c=>body+=c
        );


        req.on(
            "end",
            async()=>{


                const d =
                JSON.parse(body);



                const {data,error}=

                await supabase
                .from("levels")
                .update({

                    price:Number(d.price),
                    type:d.type,
                    active:true

                })
                .eq(
                    "id",
                    id
                )
                .select()
                .single();



                res.end(
                    JSON.stringify({
                        ok:!error,
                        level:data,
                        error:error?.message
                    })
                );


            }
        );


        return;

    }





    // DELETE LEVEL

    if(
        req.method==="DELETE" &&
        url.pathname.startsWith("/api/levels/")
    ){


        const id =
        Number(
            url.pathname.split("/").pop()
        );


        await supabase
        .from("levels")
        .delete()
        .eq(
            "id",
            id
        );


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
            service:"Binance Monitor API"
        })
    );


});



server.listen(
process.env.PORT || 10000,
()=>console.log("HTTP server started")
);





// ===============================
// BINANCE
// ===============================

const ws =
new WebSocket(
`wss://fstream.binance.com/market/stream?streams=${SYMBOL}@aggTrade`
);



ws.on(
"open",
()=>console.log("BINANCE CONNECTED")
);



ws.on(
"message",
async data=>{


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
