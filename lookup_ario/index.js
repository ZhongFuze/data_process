import express from 'express';
import { IO, ANT } from '@ar.io/sdk';

const app = express();
const io = IO.init();
const ant = ANT.init({
    processId: 'DaJgX5idVp2CnnPVUEhV9IZlwK0Q5YD24_G23q31ZX0'
  });

app.get('/lookup/arns/:name', async (req, res) => {
    const name = req.params.name;
    console.log(`lookup ${name}`);
    try {
        const record = await io.getArNSRecord({ name });
        // const info = await ant.getRecords();
        const info = await ant.getInfo();
        res.json(info);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

const PORT = 33333;
app.listen(PORT, '127.0.0.1', () => {
    console.log(`Server running on port ${PORT}`);
});