const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { Octokit } = require("@octokit/core");
const { diff } = require('json-diff');
const fetch = require('node-fetch');

const TIME_TO_WAIT_BETWEEN_GITHUB_ISSUE_CREATIONS = process.env.TIME_TO_WAIT_BETWEEN_GITHUB_ISSUE_CREATIONS || 10;  // in seconds

const s3 = new S3Client();
const octokit = new Octokit({
    auth: process.env.GITHUB_USER_TOKEN
})

async function fetchLatestStrikeData() {
    let dataString;
    if (process.env.DEBUG_LATEST_STRIKE_DATA) {
        dataString = process.env.DEBUG_LATEST_STRIKE_DATA;
    } else {
        const data = await fetch("https://striketracker.ilr.cornell.edu/geodata.js", {cache: 'no-store'});
        dataString = await data.text();
    }

    const jsonString = dataString.trim().replace(/^window\.geodata\=/, "");
    const jsonData = JSON.parse(jsonString);
    return jsonData;
}

function parseStrikeData(strikes) {
    const activeStrikes = {};

    for (const strike of strikes) {
        if (strike['Strike or Protest'] !== "Strike") {
            // we're only interested in strikes
            continue;
        }

        if (strike['End Date']) {
            // we're only interested in strikes that haven't ended yet
            continue;
        }

        activeStrikes[strike['Employer']] = strike;
    }

    return activeStrikes;
}

async function saveStrikeDataToS3(activeStrikes) {
    if (process.env.DEBUG_PREVIOUS_ACTIVE_STRIKE_DATA) {
        return;
    }

    await s3.send(new PutObjectCommand({
        Bucket: 'picket-line-discoverer-strike-data',
        Key: "active-strikes.json",
        Body: JSON.stringify(activeStrikes),
        ContentType: "application/json; charset=utf-8",
        ACL: 'public-read'
    }));
}

async function fetchSavedStrikeData() {
    let jsonString;
    if (process.env.DEBUG_PREVIOUS_ACTIVE_STRIKE_DATA) {
        jsonString = process.env.DEBUG_PREVIOUS_ACTIVE_STRIKE_DATA;
    } else {
        const data = await fetch("https://picket-line-discoverer-strike-data.s3.amazonaws.com/active-strikes.json", {cache: 'no-store'});
        jsonString = await data.text();
    }

    const jsonData = JSON.parse(jsonString);
    return jsonData;
}

function diffStrikeData(previouslyActiveStrikes, activeStrikes) {
    const diffObj = diff(previouslyActiveStrikes, activeStrikes);
    if (!diffObj) {
        return null;
    }

    const newlyActiveStrikes = [];
    const newlyInactiveStrikes = [];

    for (const [key, strike] of Object.entries(diffObj)) {
        if (key.endsWith("__added")) {
            newlyActiveStrikes.push(strike);
        } else if (key.endsWith("__deleted")) {
            newlyInactiveStrikes.push(strike);
        }
    }

    return {
        newlyActiveStrikes,
        newlyInactiveStrikes
    };
}

async function createGitHubIssue(title, strike, labels) {
    if (process.env.DEBUG_GITHUB_ISSUE_CREATION) {
        console.debug('jamespizzurro/picket-line-notifier', title, process.env.GITHUB_USER_TOKEN, JSON.stringify(strike, null, 2), labels);
    } else {
        await octokit.request('POST /repos/{owner}/{repo}/issues', {
            owner: 'jamespizzurro',
            repo: 'picket-line-notifier',
            title: title,
            body: JSON.stringify(strike, null, 2),
            labels: labels
        });
    }
}

function sleep(seconds) {
    return new Promise(resolve => setTimeout(resolve, seconds * 1000));
}

async function submitGitHubIssues(newlyActiveStrikes, newlyInactiveStrikes) {
    console.log(`Processing ${newlyActiveStrikes.length} newly active strikes...`);

    for (let i = 0; i < newlyActiveStrikes.length; i++) {
        const strike = newlyActiveStrikes[i];

        console.log(`Processing newly active strike: ${strike['Employer']}`);

        await createGitHubIssue(`Newly Active Strike: ${strike['Employer']}`, strike, ['newly active strike']);

        if (i !== newlyActiveStrikes.length - 1) {
            // there's at least one more newly active strike,
            // so wait a few seconds before trying to submit the next GitHub issue to avoid getting throttled
            await sleep(TIME_TO_WAIT_BETWEEN_GITHUB_ISSUE_CREATIONS);
        }
    }

    console.log(`Processing ${newlyInactiveStrikes.length} newly inactive strikes...`);

    for (let i = 0; i < newlyInactiveStrikes.length; i++) {
        const strike = newlyInactiveStrikes[i];

        console.log(`Processing newly inactive strike: ${strike['Employer']}`);

        await createGitHubIssue(`Newly Inactive Strike: ${strike['Employer']}`, strike, ['newly inactive strike']);

        if (i !== newlyInactiveStrikes.length - 1) {
            // there's at least one more newly inactive strike,
            // so wait a few seconds before trying to submit the next GitHub issue to avoid getting throttled
            await sleep(TIME_TO_WAIT_BETWEEN_GITHUB_ISSUE_CREATIONS);
        }
    }
}

exports.handler = async function(event, context) {
    console.log("Fetching and parsing latest strike data for active strikes...");
    const strikes = await fetchLatestStrikeData();
    const activeStrikes = parseStrikeData(strikes);

    console.log("Fetching previous active strike data...");
    const previouslyActiveStrikes = await fetchSavedStrikeData();

    if (previouslyActiveStrikes) {
        const diffData = diffStrikeData(previouslyActiveStrikes, activeStrikes);
        if (!diffData) {
            console.warn("No newly active or newly inactive strikes since we last checked. Done!");
            return;
        }

        const {newlyActiveStrikes, newlyInactiveStrikes} = diffData;
        await submitGitHubIssues(newlyActiveStrikes, newlyInactiveStrikes);
    } else {
        console.warn("No previous active strike data.");
    }

    console.info("Persisting latest active strike data to S3...");
    await saveStrikeDataToS3(activeStrikes);

    console.log("Done!");
}

// (async () => {
//     await handler();
// })();
