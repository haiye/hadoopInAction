package examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class GenerateUUID {

    /**
     * @param args
     */
    public static void main(String[] args) {
        String fileName = args[0];
        System.out.println("fileName=" + fileName);
        File file = new File(fileName);
        BufferedReader reader = null;
        ArrayList<String> uuidList = new ArrayList<String>();
        ArrayList<String> acidList = new ArrayList<String>();

        try {
            reader = new BufferedReader(new FileReader(file));
            String aLineStr = null;
            int lineNum = 1;
            while ((aLineStr = reader.readLine()) != null) {

                RawPayload rawPayload = new RawPayload(aLineStr);
                System.out.println("lineNum= " + lineNum + ": uuid=" + rawPayload.getUuid() + " acid="
                        + rawPayload.getActivityId());
                uuidList.add(rawPayload.getUuid());
                acidList.add(rawPayload.getActivityId());
                lineNum++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }

        System.out.println("UUID list=" + uuidList);
        System.out.println("acid list=" + acidList);

        ArrayList<String> saltUuidList = new ArrayList<String>();
        ArrayList<String> saltAcidList = new ArrayList<String>();
        for (String uuid : uuidList) {
            String saltKey = SaltUtils.buildStringSalt(uuid);
            saltUuidList.add(saltKey + uuid);
        }

        for (String acid : acidList) {
            String saltKey = SaltUtils.buildStringSalt(acid);
            saltAcidList.add(saltKey + acid);
        }

        System.out.println("saltUuidList list=" + saltUuidList);
        System.out.println("saltAcidList list=" + saltAcidList);

    }

    private static class RawPayload {
        public String activityId;
        public final String uuid;

        public RawPayload(String rawPayload) {
            String line = rawPayload;

            String[] tuple = line.split("\\x10");

            String[] info = tuple[10].split("\\|");

            uuid = info[0];
            activityId = info[4];
        }

        public String getActivityId() {
            return activityId;
        }

        public String getUuid() {
            return uuid;
        }

    }

}
