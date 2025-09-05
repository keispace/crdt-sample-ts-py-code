import fetch from 'node-fetch';

const hostJs = 'http://localhost:3030';
const hostPy = 'http://localhost:8000';

async function callApi(host: string, path: string): Promise<Record<string, any>> {
    const res = await fetch(`${host}${path}`);
    return res.json() as Record<string, any>;
}

async function printSnapshot(label: string, expected: number) {
    const jsSnap: Record<string, any> = await callApi(hostJs, '/get-snapshot');
    const pySnap: Record<string, any> = await callApi(hostPy, '/get-snapshot');
    const jsCount = jsSnap.snapshot?.root?.count;
    const pyCount = pySnap.snapshot?.root?.count;
    const testJs = expected === jsCount;
    const testPy = expected === pyCount;
    if (testJs && testPy) {
        console.log(`[${label}] test passed:: count is ${expected}`);
    } else {
        console.log(`[${label}] test failed:: expected count is ${expected} but ${jsCount}(js) | ${pyCount}(py)`);
    }
}

async function main() {
    await callApi(hostJs, '/init');
    await callApi(hostPy, '/init');
    await printSnapshot('After init', 1);

    await callApi(hostJs, '/add-count');
    await callApi(hostJs, '/do-sync');
    await printSnapshot('After JS/JS add-count/do-sync', 2);

    await callApi(hostJs, '/add-count');
    await callApi(hostPy, '/do-sync');
    await printSnapshot('After JS/PY add-count/do-sync', 3);

    await callApi(hostPy, '/add-count');
    await callApi(hostJs, '/do-sync');
    await printSnapshot('After PY/JS add-count/do-sync', 4);

    await callApi(hostPy, '/add-count');
    await callApi(hostPy, '/do-sync');
    await printSnapshot('After PY/PY add-count/do-sync', 5);
}

main().catch(console.error);
