CREATE DATABASE IF NOT EXISTS campus_energy;
USE campus_energy;

CREATE TABLE IF NOT EXISTS electric_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    device VARCHAR(50),
    timestamp DATETIME,
    power_kw DECIMAL(10,2),
    total_kwh DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS water_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    device VARCHAR(50),
    timestamp DATETIME,
    flow_lpm DECIMAL(10,2),
    total_l DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS gas_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    device VARCHAR(50),
    timestamp DATETIME,
    flow_m3h DECIMAL(10,2),
    total_m3 DECIMAL(10,2)
);
