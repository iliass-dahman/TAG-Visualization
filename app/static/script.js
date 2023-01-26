
// Variables

var URL = "http://localhost:";
var PORT = "8000";

var liveTraj = false
var liveStation = false
var liveSubs = false

const newSubsCanva = document.getElementById('new-subs').getContext('2d');
const TrajCanva = document.getElementById('frequented-trams').getContext('2d');
const frequentedStationsCanva = document.getElementById('frequented-stations').getContext('2d');


const selectedStation = document.getElementById('stations');

const subsNowInput = document.getElementById('subsNowInput');
const subsPastInput = document.getElementById('subsPastInput');
const trajNowInput = document.getElementById('trajNowInput');
const trajPastInput = document.getElementById('trajPastInput');
const stationNowInput = document.getElementById('stationNowInput');
const stationInput = document.getElementById('stationInput');

var subsChart = null;
var stationChart = null;
var trajChart = null;




// Build global chart
function buildChart(canva,labels, ...data) {

    const colors = [
        'rgba(255, 99, 132, 1)',
        'rgba(54, 162, 235, 1)',
        'rgba(255, 206, 86, 1)',
        'rgba(75, 192, 192, 1)',
    ];
    chart = new Chart(canva, {
        type: 'line',
        data: {
            labels: labels,
            datasets: []
        },
        options: {
            scales: {
                y: {
                beginAtZero: true
                }
            }
        }
    });
    for (let i = 0; i < data.length; i++) {
        const dataset = {
            label: data[i].label,
            data: data[i].data,
            backgroundColor: colors[i],
            borderColor: colors[i],
            lineTension: 0.4,
        }
        chart.data.datasets.push(dataset);
    }
    

    return chart;
}

// New subscribers

function getNewSubs(live) {
    if (subsChart) {
        liveSubs = false;
        subsChart.destroy();
    }
    endpoint = "new-subs?type=" + (live ? "today" : "past days");
    fetch(URL+PORT+'/' + endpoint)
    .then(res => res.json())
    .then(data => {
        
        if (live) {
            subsChart = buildChart(newSubsCanva, [data.axis], {label:"Users with monthly subscription",data:[ data.monthlyUsers ]}, {label:"Users with yearly subscription",data:[data.yearlyUsers]} );
            liveSubs = true;
        } else {
            subsChart = buildChart(newSubsCanva, data.axis, {label:"Users with monthly subscription",data:data.monthlyUsers}, {label:"Users with yearly subscription",data:data.yearlyUsers} );
        }
    })
    .catch(err => console.log(err));
}

// Trajectories
function getTrajData(live) {
    if (trajChart) {
        liveTraj = false;
        trajChart.destroy();
    }
    endpoint = "trajets?type=" + (live ? "today" : "past days");
    fetch(URL+PORT+'/' + endpoint)
    .then(res => res.json())
    .then(data => {
        
        if (live) {
            trajChart = buildChart(TrajCanva, [data.axis], {label:"TramA",data:[data.tramA]}, {label:"TramB",data:[data.tramB]}, {label:"TramC",data:[data.tramC]});
            liveTraj = true;
        } else {
            trajChart = buildChart(TrajCanva, data.axis, {label:"TramA",data:data.tramA}, {label:"TramB",data:data.tramB}, {label:"TramC",data:data.tramC} );
        }
    })
    .catch(err => console.log(err));
}

//Frequented stations
function getStationData(live) {
    if (stationChart) {
        liveStation = false;
        stationChart.destroy();
    }
    station = selectedStation.value;
    endpoint = "stations?type=" + (live ? "today" : "past days") + "&station=" + station;
    fetch(URL+PORT+'/' + endpoint)
    .then(res => res.json())
    .then(data => {
        
        if (live) {
            stationChart = buildChart(frequentedStationsCanva, [data.axis], {label:station,data:[data.users]} );
            liveStation = true;
        } else {
            stationChart = buildChart(frequentedStationsCanva, data.axis, {label:station,data:data.users} );
        }
    })
    .catch(err => console.log(err));
}

//clear station's chart
function clear(){
    stationChart.destroy();
}

// main

// Get new subs data periodically if liveSubs is true

setInterval(() => {
    if (liveSubs) {
        fetch(URL+PORT+'/new-subs?type=today')
        .then(res => res.json())
        .then(data => {
            if (subsChart.data.labels.length > 10) {
                subsChart.data.labels.shift();
                subsChart.data.datasets[0].data.shift();
                subsChart.data.datasets[1].data.shift();
            }
            subsChart.data.labels.push(data.axis);
            subsChart.data.datasets[0].data.push(data.monthlyUsers);
            subsChart.data.datasets[1].data.push(data.yearlyUsers);
            subsChart.update();
        })
        .catch(err => console.log(err));
    }
}, 10000);

// Get trajets data periodically if liveTraj is true

setInterval(() => {
    if (liveTraj) {
        fetch(URL+PORT+'/trajets?type=today')
        .then(res => res.json())
        .then(data => {
            if (trajChart.data.labels.length > 10) {
                trajChart.data.labels.shift();
                trajChart.data.datasets[0].data.shift();
                trajChart.data.datasets[1].data.shift();
                trajChart.data.datasets[2].data.shift();
            }
            trajChart.data.labels.push(data.axis);
            trajChart.data.datasets[0].data.push(data.tramA);
            trajChart.data.datasets[1].data.push(data.tramB);
            trajChart.data.datasets[2].data.push(data.tramC);
            trajChart.update();
        })
        .catch(err => console.log(err));
    }
}, 10000);

// Get frequented stations data periodically if liveStation is true

setInterval(() => {
    if (liveStation) {
        fetch(URL+PORT+'/stations?type=today&station=' + selectedStation.value)
        .then(res => res.json())
        .then(data => {
            if (stationChart.data.labels.length > 10) {
                stationChart.data.labels.shift();
                stationChart.data.datasets[0].data.shift();
            }
            stationChart.data.labels.push(data.axis);
            stationChart.data.datasets[0].data.push(data.users);
            stationChart.update();
        })
        .catch(err => console.log(err));
    }
}
, 10000);


