import express from 'express';
import { createWeb3Name } from '@web3-name-sdk/core'

const app = express();
const web3name = createWeb3Name();

app.get('/lookup/gno/:address', async (req, res) => {
    const address = req.params.address;
    console.log(`lookup ${address}`)
    try {
        const name = await web3name.getDomainName({
          address: address,
          queryChainIdList: [100],
          rpcUrl: "https://rpc.gnosischain.com",
        });
        res.json({ domainName: name });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

const PORT = 22222;
app.listen(PORT, 'localhost', () => {
  console.log(`Server running on port ${PORT}`);
});