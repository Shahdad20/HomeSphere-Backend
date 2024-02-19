const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3001;

mongoose.connect('your_mongo_uri/your_database_name', { useNewUrlParser: true, useUnifiedTopology: true });

// Define your MongoDB schema (similar to your existing schema)
const communityVacancySchema = new mongoose.Schema({
    // Define your schema fields
    // Example: communityname, latitude, longitude, apt_vacant, cnv_vacant, ...
});

const CommunityVacancy = mongoose.model('CommunityVacancy', communityVacancySchema);

app.use(cors());

// Define an API endpoint to retrieve the community vacancy data
app.get('/api/community-vacancy', async (req, res) => {
    try {
        // Fetch and process data from MongoDB (replace with your actual query logic)
        const communityVacancyData = await CommunityVacancy.find().lean();

        // Send the processed data as JSON
        res.json(communityVacancyData);
    } catch (error) {
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
