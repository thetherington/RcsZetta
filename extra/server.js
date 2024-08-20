import express from "express";
import fs from "fs";

const stations = fs.readFileSync("./payloads/get-stationlist.json", {
    encoding: "utf8",
    flag: "r",
});

const orglist = fs.readFileSync("./payloads/get-orglist.json", {
    encoding: "utf8",
    flag: "r",
});

const stationStatus = fs.readFileSync("./payloads/get-onairstatus.json", {
    encoding: "utf8",
    flag: "r",
});

const app = express();
const port = 3000;

app.get("/1.0/Station/list", (req, res) => res.send(stations));

app.get("/1.0/Organization/list", (req, res) => res.send(orglist));

app.get("/1.0/StationScheduleLog/OnAir/Status/:uuid", (req, res) =>
    res.send(stationStatus)
);

app.listen(port, () => console.log(`Example app listening on port ${port}!`));
