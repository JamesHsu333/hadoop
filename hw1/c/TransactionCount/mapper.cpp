#include <iostream>
#include <string>
using namespace std;

int main() {
    string line;
    while(getline(cin, line)){
        // Trim the last whitespace from raw data
        line.erase(line.end()-1, line.end());
        cout << line << endl;
    }
    return 0;
}
