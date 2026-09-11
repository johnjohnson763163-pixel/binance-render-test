import http from "http";
import WebSocket from "ws";
console.log("SUPABASE CHECK:", {
    url: !!process.env.SUPABASE_URL,
    key: !!process.env.SUPABASE_SERVICE_KEY
});


const SYMBOL = "solusdt";


let LEVELS = [];

let nextId = 1;



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



        if(level.type==="above"){


            if(
                price >= level.price &&
                !level.active
            ){

                level.active=true;


                sendTelegram(
                    `${level.symbol} пробил уровень ${level.price}. Цена ${price}`
                );

            }



            if(price < level.price){

                level.active=false;

            }


        }




        if(level.type==="below"){


            if(
                price <= level.price &&
                !level.active
            ){

                level.active=true;


                sendTelegram(
                    `${level.symbol} ниже уровня ${level.price}. Цена ${price}`
                );

            }



            if(price > level.price){

                level.active=false;

            }


        }


    }

}







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



    // GET ALL LEVELS

    if(
        req.method==="GET" &&
        url.pathname==="/api/levels"
    ){

        res.end(
            JSON.stringify(LEVELS)
        );

        return;

    }





    // ADD LEVEL

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
            ()=>{


                const data =
                JSON.parse(body);



                const level={

                    id: nextId++,
                    symbol:data.symbol,
                    price:Number(data.price),
                    type:data.type,
                    active:false

                };



                LEVELS.push(level);



                console.log(
                    "LEVEL ADDED",
                    level
                );



                res.end(
                    JSON.stringify({
                        ok:true,
                        level
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



        LEVELS =
        LEVELS.filter(
            x=>x.id!==id
        );



        res.end(
            JSON.stringify({
                ok:true
            })
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
            ()=>{


                const data =
                JSON.parse(body);



                const level =
                LEVELS.find(
                    x=>x.id===id
                );



                if(level){


                    level.price =
                    Number(data.price ?? level.price);


                    level.type =
                    data.type ?? level.type;


                    level.active=false;


                }



                res.end(
                    JSON.stringify({
                        ok:true,
                        level
                    })
                );


            }
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
data=>{


    const msg =
    JSON.parse(data);



    const price =
    Number(msg.data.p);



    console.log(
        SYMBOL,
        price
    );



    checkLevels(price);


});



ws.on(
"error",
e=>console.log(
"WS ERROR",
e.message
)
);
