import { Button } from "./ui";
import { Card } from "./ui";
import { Calculator, Users, Shield, MessageCircle } from "lucide-react";
import { Link } from "./navigation";

export const Hero = () => {
  return (
    <section className="relative min-h-screen bg-gradient-to-br from-[#103b3b] via-[#155a5b] to-[#0b2933] flex items-center justify-center overflow-hidden">
      {/* Patrón de fondo */}
      <div className="absolute inset-0 opacity-10">
        <div className="absolute inset-0" style={{
          backgroundImage: `url("data:image/svg+xml,%3Csvg width='60' height='60' viewBox='0 0 60 60' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='none' fill-rule='evenodd'%3E%3Cg fill='%23ffffff' fill-opacity='0.4'%3E%3Cpath d='M36 34v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zm0-30V0h-2v4h-4v2h4v4h2V6h4V4h-4zM6 34v-4H4v4H0v2h4v4h2v-4h4v-2H6zM6 4V0H4v4H0v2h4v4h2V6h4V4H6z'/%3E%3C/g%3E%3C/g%3E%3C/svg%3E")`,
        }} />
      </div>

      <div className="absolute inset-0 bg-black/5"></div>

      <div className="container mx-auto px-4 py-20 relative z-10">
        <div className="grid lg:grid-cols-2 gap-12 items-center">
          <div className="text-center lg:text-left">
            {/* Badge */}
            <div className="inline-flex items-center gap-2 bg-white/20 backdrop-blur-sm px-4 py-2 rounded-full mb-6">
              <Shield className="h-4 w-4 text-white" />
              <span className="text-white text-sm font-medium">Sin registro para empezar</span>
            </div>

            <h1 className="text-5xl lg:text-7xl font-bold text-white mb-6 leading-tight">
              Divide gastos en tus
                <span className="block bg-gradient-to-r from-emerald-100 to-amber-100 bg-clip-text text-transparent">
                chats de grupo
              </span>
            </h1>

            <p className="text-xl lg:text-2xl text-white/90 mb-8 leading-relaxed">
              Registrá, dividí y seguí gastos compartidos desde la web.
              <span className="font-semibold"> Sin registros para empezar, sin planillas y sin complicaciones.</span>
            </p>

            <div className="flex flex-col sm:flex-row gap-4 justify-center lg:justify-start mb-12">
              <Button asChild size="lg" className="classic-start bg-amber-300 text-slate-950 hover:bg-amber-200 text-lg px-8 py-6 rounded-full shadow-lg shadow-black/20 font-semibold">
                <Link to="/dividir">
                  <Calculator className="mr-2 h-5 w-5" />
                  Empezar a dividir
                </Link>
              </Button>
              <Button asChild size="lg" variant="outline" className="border-2 border-white/70 !bg-transparent !text-white hover:!bg-white/10 hover:!text-white text-lg px-8 py-6 rounded-full">
                <a href="#how-it-works">
                  <MessageCircle className="mr-2 h-5 w-5" />
                  Cómo funciona
                </a>
              </Button>
            </div>

            <div className="flex flex-wrap gap-3 justify-center lg:justify-start mb-8">
              <div className="inline-flex items-center gap-2 rounded-full border border-white/25 bg-white/10 px-4 py-2 text-sm text-white">
                <MessageCircle className="h-4 w-4 text-sky-200" />
                División rápida
              </div>
              <div className="inline-flex items-center gap-2 rounded-full border border-emerald-300/40 bg-emerald-400/15 px-4 py-2 text-sm text-white">
                <MessageCircle className="h-4 w-4 text-emerald-200" />
                Grupos y finanzas
              </div>
            </div>

            <div className="flex flex-wrap justify-center lg:justify-start gap-8 text-white/90">
              <div className="flex items-center gap-2">
                <Users className="h-5 w-5" />
                <span>Grupos ilimitados</span>
              </div>
              <div className="flex items-center gap-2">
                <Calculator className="h-5 w-5" />
                <span>Cálculo automático</span>
              </div>
              <div className="flex items-center gap-2">
                <MessageCircle className="h-5 w-5" />
                <span>Web + Telegram</span>
              </div>
            </div>
          </div>

          <div className="relative">
            <Card className="bg-white/10 backdrop-blur-md p-8 shadow-2xl rounded-3xl border border-white/20">
              {/* Mock de chat grupal */}
              <div className="bg-slate-950 rounded-2xl overflow-hidden shadow-lg">
                {/* Header del chat */}
                <div className="bg-slate-900 px-4 py-3 flex items-center gap-3">
                  <div className="w-10 h-10 rounded-full bg-gradient-to-br from-emerald-400 to-teal-500 flex items-center justify-center">
                    <span className="text-white font-bold">S</span>
                  </div>
                  <div>
                    <div className="text-white font-semibold">Viaje a Córdoba</div>
                    <div className="text-gray-400 text-sm">Ejemplo · grupo compartido</div>
                  </div>
                </div>

                {/* Mensajes */}
                <div className="p-4 space-y-3 min-h-[300px]">
                  {/* Mensaje del usuario */}
                  <div className="flex justify-end">
                    <div className="bg-teal-700 text-white px-4 py-2 rounded-2xl rounded-br-md max-w-[80%]">
                      Cargué la cena · $15.000
                    </div>
                  </div>

                  {/* Respuesta del bot */}
                  <div className="flex justify-start">
                    <div className="bg-slate-900 text-white px-4 py-3 rounded-2xl rounded-bl-md max-w-[85%]">
                      <div className="text-green-400 mb-1">✅ Gasto registrado</div>
                      <div className="space-y-1 text-sm">
                        <div>📝 <span className="font-semibold">Cena</span></div>
                        <div>💰 Monto: $15,000</div>
                        <div>👤 Pagado por: Juan</div>
                      </div>
                      <div className="text-gray-400 text-xs mt-2">
                        ¿Lo dividimos entre quienes participaron?
                      </div>
                    </div>
                  </div>

                  {/* Otro mensaje */}
                  <div className="flex justify-end">
                    <div className="bg-teal-700 text-white px-4 py-2 rounded-2xl rounded-br-md max-w-[80%]">
                      Sí, dividir entre todos
                    </div>
                  </div>

                  {/* Respuesta división */}
                  <div className="flex justify-start">
                    <div className="bg-slate-900 text-white px-4 py-3 rounded-2xl rounded-bl-md max-w-[85%]">
                      <div className="mb-2">💰 <span className="font-semibold">Gasto dividido: Cena</span></div>
                      <div className="text-sm space-y-1">
                        <div>📊 Total: $15,000</div>
                        <div>👥 Participantes: 3</div>
                        <div className="text-cyan-400 font-semibold">💵 Por persona: $5,000</div>
                      </div>
                    </div>
                  </div>

                  <div className="flex justify-start">
                    <div className="bg-slate-900 text-white px-4 py-3 rounded-2xl rounded-bl-md max-w-[85%]">
                      <div className="mb-1 font-semibold">✓ Resumen listo para compartir</div>
                      <div className="text-sm text-emerald-300">Copialo o compartilo con el grupo</div>
                    </div>
                  </div>
                </div>
              </div>
            </Card>

            <div className="absolute -top-4 -right-4 bg-green-500 text-white px-6 py-3 rounded-full shadow-lg font-semibold animate-pulse">
              ¡Gratis!
            </div>
          </div>
        </div>
      </div>
    </section>
  );
};
