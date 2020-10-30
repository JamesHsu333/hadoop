#include <iostream>
#include <string>
using namespace std;

int main() {
    string line;
    string delimiter = " ";
    string key;
    string value;
    size_t position = 0;

    while(getline(cin, line)){
        // Trim the last whitespace from raw data
        line.erase(line.end()-1, line.end());
        value = line;
        while((position = line.find(delimiter)) != string::npos){
            key = line.substr(0, position);
            cout << key << " " << value << endl;
            line.erase(0, position + delimiter.length());
        }
        cout << line << " " << value << endl;
    }
    return 0;
}
