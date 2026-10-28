import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class GenerateTestFile {
    public static void main(String[] args) throws Exception {
        File src = new File("input.txt");
        if (!src.exists()) {
            src = new File("../KGP_Transfer_Protocol/input.txt");
        }
        if (!src.exists()) {
            System.err.println("Source input.txt not found");
            return;
        }

        byte[] srcBytes;
        try (FileInputStream fis = new FileInputStream(src)) {
            srcBytes = fis.readAllBytes();
        }

        // Copy input.txt locally if not present
        File localInput = new File("input.txt");
        if (!localInput.exists()) {
            try (FileOutputStream fos = new FileOutputStream(localInput)) {
                fos.write(srcBytes);
            }
        }

        // Create large_input.txt (> 100 KB, e.g. ~142 KB = 8 repetitions)
        File largeInput = new File("large_input.txt");
        try (FileOutputStream fos = new FileOutputStream(largeInput)) {
            for (int i = 1; i <= 8; i++) {
                fos.write(("=== BLOCK " + i + " START ===\n").getBytes());
                fos.write(srcBytes);
                fos.write(("\n=== BLOCK " + i + " END ===\n\n").getBytes());
            }
        }
        System.out.println("Generated large_input.txt: " + largeInput.length() + " bytes (> 100 KB)");
    }
}
