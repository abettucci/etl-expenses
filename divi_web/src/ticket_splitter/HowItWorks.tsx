import { Card, CardContent } from "./ui";
import { UserPlus, Receipt, Calculator, Bell, WalletCards, Scale, CircleCheckBig } from "lucide-react";

const steps = [
  {
    icon: UserPlus,
    step: "1",
    title: "Creá una división",
    description: "Poné un nombre al plan y agregá a las personas que van a participar."
  },
  {
    icon: Receipt,
    step: "2",
    title: "Cargá los gastos",
    description: "Indicá qué se pagó, el total y cuánto adelantó cada persona."
  },
  {
    icon: Calculator,
    step: "3",
    title: "División automática",
    description: "Elegí partes iguales o importes exactos para cada participante."
  },
  {
    icon: Bell,
    step: "4",
    title: "Compartí y saldá",
    description: "Compartí el resumen y marcá las transferencias realizadas en tu división."
  }
];

export const HowItWorks = () => {
  return (
    <section id="how-it-works" className="py-20 bg-white">
      <div className="container mx-auto px-4">
        <div className="text-center mb-16">
          <h2 className="text-4xl lg:text-5xl font-bold text-slate-900 mb-6">
            ¿Cómo funciona
            <span className="bg-gradient-to-r from-teal-700 to-emerald-500 bg-clip-text text-transparent"> Splitter</span>?
          </h2>
          <p className="text-xl text-slate-600 max-w-2xl mx-auto">
            En 4 pasos simples tendrás todos los gastos de tu grupo organizados y divididos automáticamente.
          </p>
        </div>

        <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-8">
          {steps.map((step, index) => (
            <div key={index} className="text-center relative">
              <Card className="bg-gradient-to-br from-slate-50 to-white shadow-lg border border-slate-100 p-8 hover:shadow-xl transition-all duration-300 group relative z-10">
                <CardContent className="p-0">
                  <div className="relative mb-6">
                    <div className="bg-gradient-to-br from-teal-700 to-emerald-500 p-4 rounded-2xl w-20 h-20 mx-auto flex items-center justify-center group-hover:scale-110 transition-transform duration-300 shadow-lg shadow-emerald-900/15">
                      <step.icon className="h-8 w-8 text-white" />
                    </div>
                    <div className="absolute -top-2 -right-2 bg-amber-400 text-amber-900 rounded-full w-8 h-8 flex items-center justify-center text-sm font-bold shadow">
                      {step.step}
                    </div>
                  </div>
                  <h3 className="text-xl font-semibold text-slate-900 mb-4">{step.title}</h3>
                  <p className="text-slate-600 leading-relaxed">{step.description}</p>
                </CardContent>
              </Card>

              {index < steps.length - 1 && (
                <div className="hidden lg:block absolute top-1/2 -right-4 transform -translate-y-1/2 z-0">
                  <div className="w-8 h-1 bg-gradient-to-r from-teal-700 to-emerald-500 rounded-full"></div>
                </div>
              )}
            </div>
          ))}
        </div>

        <div className="mt-20 bg-slate-900 rounded-3xl p-8 lg:p-12">
          <div className="max-w-2xl mx-auto text-center mb-8">
            <h3 className="text-2xl lg:text-3xl font-bold text-white">
              En tu división, podés…
            </h3>
            <p className="text-slate-400 mt-3">
              Hacer las cuentas desde el navegador y compartir el resultado con el grupo.
            </p>
          </div>
          <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-4 max-w-4xl mx-auto">
            {[
              { icon: Receipt, title: "Cargar un gasto", desc: "Contá qué se pagó y cuánto." },
              { icon: WalletCards, title: "Ver los gastos", desc: "Revisá lo que lleva el grupo." },
              { icon: Scale, title: "Dividir en un toque", desc: "Repartí entre quienes participaron." },
              { icon: CircleCheckBig, title: "Marcar un pago", desc: "Mantené las cuentas al día." },
              { icon: Calculator, title: "Ver el balance", desc: "Sabé quién debe y quién recibe." },
              { icon: Bell, title: "Compartir el resumen", desc: "Copiá las transferencias pendientes o compartilas con el grupo." },
            ].map((item, index) => (
              <div key={index} className="bg-slate-800 rounded-xl p-5 border border-white/5">
                <item.icon className="h-5 w-5 text-emerald-300 mb-3" />
                <h4 className="text-white font-semibold">{item.title}</h4>
                <p className="text-slate-400 text-sm mt-1">{item.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
};
