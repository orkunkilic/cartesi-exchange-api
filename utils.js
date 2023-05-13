"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.hex2str = void 0;
const ethers_1 = require("ethers");
const hex2str = (hex) => {
    try {
        return ethers_1.ethers.toUtf8String(hex);
    }
    catch (e) {
        console.log(e);
        // cannot decode hex payload as a UTF-8 string
        return hex;
    }
};
exports.hex2str = hex2str;
