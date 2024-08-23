const fs = require('fs').promises;
const path = require('path');
const axios = require('axios');
const https = require('https');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
require('dotenv').config();

const {
    SOURCE_DIRECTORY,
    BASE_URL,
    DESTINATION_DIRECTORY,
    WORKER_LIMIT,
    HEAD_REQUEST_TIMEOUT,
    GET_REQUEST_TIMEOUT,
    RETRY_COUNT,
    RETRY_DELAY
} = process.env;

const httpsAgent = new https.Agent({
    rejectUnauthorized: false
});

async function getFiles(dir) {
    const results = [];
    const list = await fs.readdir(dir, { withFileTypes: true });
    await Promise.all(list.map(async (dirent) => {
        const filePath = path.join(dir, dirent.name);
        if (dirent.isDirectory()) {
            const res = await getFiles(filePath);
            results.push(...res);
        } else {
            results.push(filePath);
        }
    }));
    return results;
}

async function checkFileExists(url) {
    try {
        console.log(`Đang kiểm tra URL: ${url}`);
        const response = await axios.get(url, {
            timeout: +HEAD_REQUEST_TIMEOUT,
            httpsAgent,
            validateStatus: (status) => status < 500 // Chỉ từ chối nếu status >= 500
        });
        console.log(`Phản hồi từ máy chủ: ${response.status}`);
        return response.status === 200 || response.status === 201; // Chấp nhận cả 200 và 201
    } catch (error) {
        console.error(`Lỗi khi kiểm tra URL: ${url} - ${error.message}`);
        return false;
    }
}

async function downloadFile(url, dest, retryCount = +RETRY_COUNT) {
    for (let i = 0; i <= retryCount; i++) {
        try {
            const response = await axios({
                url,
                method: 'GET',
                responseType: 'stream',
                timeout: +GET_REQUEST_TIMEOUT,
                httpsAgent
            });

            await fs.mkdir(path.dirname(dest), { recursive: true });

            await new Promise((resolve, reject) => {
                const writer = require('fs').createWriteStream(dest);
                response.data.pipe(writer);
                writer.on('finish', resolve);
                writer.on('error', reject);
            });
            return;
        } catch (error) {
            if (i < retryCount) {
                console.log(`Lỗi tải xuống, thử lại lần ${i + 1}...`);
                await new Promise(resolve => setTimeout(resolve, +RETRY_DELAY));
            } else {
                throw error;
            }
        }
    }
}

if (isMainThread) {
    async function processFiles(srcDir, baseURL, destDir) {
        try {
            const files = await getFiles(srcDir);
            console.log(`Đã tìm thấy ${files.length} tệp trong thư mục nguồn.`);

            const workers = [];
            for (let i = 0; i < Math.min(+WORKER_LIMIT, files.length); i++) {
                workers.push(new Promise((resolve, reject) => {
                    const worker = new Worker(__filename, {
                        workerData: { files, baseURL, destDir, start: i, step: +WORKER_LIMIT }
                    });

                    worker.on('message', (message) => {
                        console.log(message);
                        resolve();
                    });
                    worker.on('error', reject);
                    worker.on('exit', (code) => {
                        if (code !== 0) {
                            reject(new Error(`Worker kết thúc với mã thoát ${code}`));
                        }
                    });
                }));
            }

            await Promise.all(workers);
            console.log('Tất cả các tệp đã được xử lý.');
        } catch (error) {
            console.error('Lỗi khi xử lý tệp:', error);
        }
    }

    async function main() {
        await processFiles(SOURCE_DIRECTORY, BASE_URL, DESTINATION_DIRECTORY);
    }

    main().catch(console.error);
} else {
    (async () => {
        const { files, baseURL, destDir, start, step } = workerData;

        for (let i = start; i < files.length; i += step) {
            try {
                const relativePath = path.relative(SOURCE_DIRECTORY, files[i]);
                const destPath = path.join(destDir, relativePath);
                const fileUrl = `${baseURL}${relativePath.replace(/\\/g, '/')}`;

                if (await checkFileExists(fileUrl)) {
                    console.log(`Đang tải tệp: ${fileUrl} về ${destPath}`);
                    await downloadFile(fileUrl, destPath);
                    console.log(`Đã tải tệp: ${fileUrl} về ${destPath}`);
                } else {
                    console.log(`Tệp không tồn tại: ${fileUrl}`);
                }
            } catch (error) {
                console.error(`Lỗi khi xử lý tệp: ${files[i]} - ${error.message}`);
            }
        }

        parentPort.postMessage('Hoàn thành xử lý tệp.');
    })();
}