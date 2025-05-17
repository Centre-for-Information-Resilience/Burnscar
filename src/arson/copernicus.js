//VERSION=3
const colorRamp = [[0, 0x140b34], [0.25, 0x84206b], [0.75, 0xe55c30], [1, 0xf6d746]]

let viz = new ColorRampVisualizer(colorRamp);

function setup() {
    return {
        input: ["B08", "B12", "dataMask"],
        output: [
            { id: "default", bands: 4 },
            { id: "index", bands: 1, sampleType: 'FLOAT32' }
        ]
    };
}

function evaluatePixel(samples) {
    let index = (samples.B08 - samples.B12) / (samples.B08 + samples.B12);
    const minIndex = 0;
    const maxIndex = 1;
    let visVal = null;

    if (index > maxIndex || index < minIndex) {
        visVal = [0, 0, 0, 0];
    }
    else {
        visVal = [...viz.process(index), samples.dataMask];
    }

    // The library for tiffs only works well if there is one channel returned.
    // So here we encode "no data" as NaN and ignore NaNs on the frontend.  
    const indexVal = samples.dataMask === 1 ? index : NaN;

    return { default: visVal, index: [indexVal] };
}