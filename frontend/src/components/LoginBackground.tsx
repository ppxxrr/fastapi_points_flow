import backgroundImage from "../../background.jpg";

export default function LoginBackground() {
    return (
        <>
            <div
                className="pointer-events-none absolute inset-0 bg-cover bg-center bg-no-repeat"
                style={{ backgroundImage: `url(${backgroundImage})` }}
            />
            <div className="pointer-events-none absolute inset-0 bg-[linear-gradient(180deg,rgba(248,251,255,0.3),rgba(241,246,255,0.42),rgba(245,248,255,0.34))]" />
            <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_50%_44%,rgba(255,255,255,0.22),transparent_24%),radial-gradient(circle_at_50%_50%,rgba(191,219,254,0.12),transparent_38%)] backdrop-blur-[1.5px]" />
        </>
    );
}
