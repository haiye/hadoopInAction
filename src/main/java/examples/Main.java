package examples;

import java.util.Arrays;


/*
 * 
 * 



6e284688613f47a898e9351451d833d8
2d480ac67a8044a3abe3792ba60568ae 
a305e6012b6e47b58071ae750e8eec0d
9eaa9cec139b498c8a58ea6bec679a19
936ca520f599473aa4866f3ae16d3a61
36467 f8068e63547e467188856ea5f79985f9

454e1f2341254c6e860c0e5fd3ece482
d7c8a267ce754492bdda0c346e289738
d2a683558fa14275bce1bad03e7ac100
a9b3f31fd3d94a54a4b83a9b0f2dfa62
40af26e6b57a4658831f0a14e842423b
936ca520f599473aa4866f3ae16d3a61
8f6a3b716a764aa0b3b6150636108f2e
d2a683558fa14275bce1bad03e7ac100
69b5e01f95fc454588a4de8cd696749f
2d5b4ef83a9c46db95ba58fdd92a6d2b

get 'SC_IRAS_BREFCP_UUID_INDEX', '08897bbbbb1cac54649ee86685e08d4790d88', {COLUMN=>'C'}
bbbbb1cac54649ee86685e08d4790d88
 * 
 * */
public class Main {

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
    String uuid="bbbbb1cac54649ee86685e08d4790d88";
    
    String keyBytes = SaltUtils.buildStringSalt(uuid);
    System.out.println(keyBytes+uuid);
    }

}
