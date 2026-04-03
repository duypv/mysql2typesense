import mysql from "mysql2/promise";
import { execSync } from "node:child_process";

const TS_BASE = process.env.TS_BASE ?? "http://127.0.0.1:8108";
const TS_API_KEY = process.env.TS_API_KEY ?? "xyz";
const headers = { "X-TYPESENSE-API-KEY": TS_API_KEY };

const pool = await mysql.createPool({
  host: process.env.MYSQL_HOST ?? "127.0.0.1",
  port: Number(process.env.MYSQL_PORT ?? "3306"),
  user: process.env.MYSQL_USER ?? "root",
  password: process.env.MYSQL_PASS ?? "root",
  multipleStatements: true,
});

async function ts(path, init = {}) {
  const res = await fetch(`${TS_BASE}${path}`, {
    ...init,
    headers: { ...headers, ...(init.headers ?? {}) },
  });
  return res;
}

async function ensureMockMysqlData() {
  const sql = `
CREATE DATABASE IF NOT EXISTS app_mock;
USE app_mock;

DROP TABLE IF EXISTS Appointment;
DROP TABLE IF EXISTS ServicePriceHistory;
DROP TABLE IF EXISTS DrugPriceHistory;
DROP TABLE IF EXISTS Service;
DROP TABLE IF EXISTS Drug;
DROP TABLE IF EXISTS Doctor;
DROP TABLE IF EXISTS Patient;

CREATE TABLE Service (
  ServiceID INT PRIMARY KEY,
  ServiceName VARCHAR(255),
  Type VARCHAR(100),
  Status VARCHAR(50),
  Category VARCHAR(100)
);

CREATE TABLE Drug (
  DrugID INT PRIMARY KEY,
  DrugName VARCHAR(255),
  Type VARCHAR(100),
  Status VARCHAR(50)
);

CREATE TABLE Doctor (
  DoctorID INT PRIMARY KEY,
  DoctorName VARCHAR(255),
  Gender VARCHAR(20),
  Status VARCHAR(50)
);

CREATE TABLE Patient (
  PatientID INT PRIMARY KEY,
  PatientName VARCHAR(255),
  Gender VARCHAR(20),
  Status VARCHAR(50)
);

CREATE TABLE ServicePriceHistory (
  ID INT PRIMARY KEY,
  ServiceID INT,
  Price DECIMAL(10,2),
  Type VARCHAR(100),
  Status VARCHAR(50),
  Note VARCHAR(255)
);

CREATE TABLE DrugPriceHistory (
  ID INT PRIMARY KEY,
  DrugID INT,
  Price DECIMAL(10,2),
  Type VARCHAR(100),
  Status VARCHAR(50)
);

CREATE TABLE Appointment (
  ID INT PRIMARY KEY,
  DoctorID INT,
  PatientID INT,
  Status VARCHAR(50),
  MedicalServiceID INT,
  WeekNumber INT
);

INSERT INTO Service (ServiceID, ServiceName, Type, Status, Category) VALUES
  (101, 'Blood Test', 'LAB', 'ACTIVE', 'TEST'),
  (102, 'X-Ray', 'IMG', 'ACTIVE', 'IMAGING');

INSERT INTO Drug (DrugID, DrugName, Type, Status) VALUES
  (201, 'Paracetamol', 'ANALGESIC', 'ACTIVE'),
  (202, 'Amoxicillin', 'ANTIBIOTIC', 'ACTIVE');

INSERT INTO Doctor (DoctorID, DoctorName, Gender, Status) VALUES
  (301, 'Dr. A', 'MALE', 'ACTIVE'),
  (302, 'Dr. B', 'FEMALE', 'ACTIVE');

INSERT INTO Patient (PatientID, PatientName, Gender, Status) VALUES
  (401, 'Patient One', 'MALE', 'ACTIVE'),
  (402, 'Patient Two', 'FEMALE', 'ACTIVE');

INSERT INTO ServicePriceHistory (ID, ServiceID, Price, Type, Status, Note) VALUES
  (1, 101, 120000, 'LAB', 'ACTIVE', 'first service history'),
  (2, 102, 250000, 'IMG', 'ACTIVE', 'second service history');

INSERT INTO DrugPriceHistory (ID, DrugID, Price, Type, Status) VALUES
  (1, 201, 15000, 'ANALGESIC', 'ACTIVE'),
  (2, 202, 23000, 'ANTIBIOTIC', 'ACTIVE');

INSERT INTO Appointment (ID, DoctorID, PatientID, Status, MedicalServiceID, WeekNumber) VALUES
  (1, 301, 401, 'BOOKED', 101, 14),
  (2, 302, 402, 'CONFIRMED', 102, 14);
`;

  await pool.query(sql);
}

async function deleteCollection(name) {
  const res = await ts(`/collections/${name}`, { method: "DELETE" });
  if (res.status !== 200 && res.status !== 404) {
    const body = await res.text();
    throw new Error(`Delete collection ${name} failed: ${res.status} ${body}`);
  }
}

async function getCollection(name) {
  const res = await ts(`/collections/${name}`);
  if (res.status !== 200) {
    const body = await res.text();
    throw new Error(`Get collection ${name} failed: ${res.status} ${body}`);
  }
  return res.json();
}

function getField(schema, name) {
  return schema.fields.find((f) => f.name === name);
}

async function main() {
  console.log("[1/4] Preparing mock MySQL data...");
  await ensureMockMysqlData();

  console.log("[2/4] Cleaning target Typesense collections...");
  for (const c of [
    "Service",
    "Drug",
    "Doctor",
    "Patient",
    "ServicePriceHistory",
    "DrugPriceHistory",
    "Appointment",
  ]) {
    await deleteCollection(c);
  }

  console.log("[3/4] Running sync:initial with mock config...");
  execSync("npm run sync:initial", {
    stdio: "inherit",
    env: {
      ...process.env,
      DB_HOST: process.env.DB_HOST ?? process.env.MYSQL_HOST ?? "127.0.0.1",
      DB_PORT: process.env.DB_PORT ?? process.env.MYSQL_PORT ?? "3306",
      DB_USER: process.env.DB_USER ?? process.env.MYSQL_USER ?? "root",
      DB_PASS: process.env.DB_PASS ?? process.env.MYSQL_PASS ?? "root",
      DB_NAME: "app_mock",
      TS_NODE_HOST: "127.0.0.1",
      TS_NODE_PORT: "8108",
      TS_NODE_PROTOCOL: "http",
      TS_API_KEY: TS_API_KEY,
      CHECKPOINT_DRIVER: "file",
      CHECKPOINT_FILE: "storage/checkpoints/mock-facet-join-checkpoint.json",
      SYNC_CONFIG_PATH: "config/sync.mock-facet-join.config.json",
      MONITORING_ENABLED: "false",
    },
  });

  console.log("[4/4] Validating facet + join fields from schema and query...");
  const servicePriceHistory = await getCollection("ServicePriceHistory");
  const drugPriceHistory = await getCollection("DrugPriceHistory");
  const appointment = await getCollection("Appointment");

  const spServiceId = getField(servicePriceHistory, "ServiceID");
  const dpDrugId = getField(drugPriceHistory, "DrugID");
  const apDoctorId = getField(appointment, "DoctorID");
  const apPatientId = getField(appointment, "PatientID");
  const apStatus = getField(appointment, "Status");
  const apMedicalServiceId = getField(appointment, "MedicalServiceID");

  const checks = [
    ["ServicePriceHistory.ServiceID is facet", spServiceId?.facet === true],
    ["ServicePriceHistory.ServiceID reference=Service.ServiceID", spServiceId?.reference === "Service.ServiceID"],
    ["ServicePriceHistory.ServiceID type=string", spServiceId?.type === "string"],
    ["DrugPriceHistory.DrugID is facet", dpDrugId?.facet === true],
    ["DrugPriceHistory.DrugID reference=Drug.DrugID", dpDrugId?.reference === "Drug.DrugID"],
    ["Appointment.DoctorID reference=Doctor.DoctorID", apDoctorId?.reference === "Doctor.DoctorID"],
    ["Appointment.PatientID reference=Patient.PatientID", apPatientId?.reference === "Patient.PatientID"],
    ["Appointment.Status is facet", apStatus?.facet === true],
    ["Appointment.MedicalServiceID is facet", apMedicalServiceId?.facet === true],
  ];

  for (const [name, pass] of checks) {
    console.log(`${pass ? "PASS" : "FAIL"} - ${name}`);
  }

  const qp = new URLSearchParams({
    q: "*",
    query_by: "Note",
    include_fields: "ServiceID,Note,$Service(ServiceID,ServiceName,Type)",
    per_page: "10",
  });
  const joinRes = await ts(`/collections/ServicePriceHistory/documents/search?${qp.toString()}`);
  const joinBody = await joinRes.json();

  const firstDoc = joinBody.hits?.[0]?.document;
  const hasJoin = firstDoc && firstDoc.Service && firstDoc.Service.ServiceID;

  console.log(`Join query status: ${joinRes.status}`);
  console.log(`Join data present: ${hasJoin ? "YES" : "NO"}`);

  const failed = checks.filter(([, pass]) => !pass).length;
  if (failed > 0 || !hasJoin || joinRes.status !== 200) {
    throw new Error(`Validation failed: failedChecks=${failed}, joinStatus=${joinRes.status}, joinData=${!!hasJoin}`);
  }

  console.log("All facet/join validations passed.");
}

try {
  await main();
} finally {
  await pool.end();
}
