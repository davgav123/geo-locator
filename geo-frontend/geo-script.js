
function processData() {
    var country = document.getElementById("inputCountry").value
    var filterCondition = document.getElementById("inputCondition").value

    // TODO: check validity of inputs

    httpGet(
        "http://localhost:8080/selection?country=" + country + "&param=" + filterCondition,
        function(err, returnedData) {
            if (err) {
                console.log("error text:\n", err)
                return ;
            }

            console.log(returnedData)
        }
    )
}

function httpGet(url, callback) {
    const httpReq = new XMLHttpRequest();
    httpReq.onreadystatechange = function() {
        if (httpReq.readyState == 4) {
            if (httpReq.status == 200) {
                const data = httpReq.responseText;
                callback(null, data);
            } else {
                const err = new Error(httpReq.statusText);
                callback(err, null);
            }
        }
    };

    httpReq.open("GET", url, true);
    httpReq.send();
}
